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
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const raft_engine = @import("raft_engine");
const metadata_api = @import("../metadata/api.zig");
const metadata_http_client = @import("../metadata/http_client.zig");
const metadata_http_server = @import("../metadata/http_server.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_service = @import("../metadata/service.zig");
const metadata_sim = @import("../metadata/sim_harness.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_table_workflow = @import("../metadata/table_workflow.zig");
const raft_catalog = @import("../raft/catalog.zig");
const raft_host = @import("../raft/host.zig");
const raft_sim = @import("../raft/sim_harness.zig");
const http_common = @import("../raft/transport/http_common.zig");
const std_http_executor = @import("../raft/transport/std_http_executor.zig");
const std_http_listener = @import("../raft/transport/std_http_listener.zig");
const api_http_client = @import("http_client.zig");
const api_routes = @import("http_routes.zig");
const api_http_server = @import("http_server.zig");
const api_table_catalog = @import("table_catalog.zig");
const api_table_reads = @import("table_reads.zig");
const api_table_router = @import("table_router.zig");
const api_table_writes = @import("table_writes.zig");
const api_tables = @import("tables.zig");
const test_contract_helpers = @import("test_contract_helpers.zig");
const indexes_api = @import("indexes.zig");
const db_mod = @import("../storage/db/mod.zig");
const docstore_mod = @import("../storage/docstore.zig");
const transactions_mod = @import("../storage/transactions.zig");
const distributed_txn = @import("distributed_txn.zig");
const transactions_api = @import("transactions.zig");

fn parsePageJson(comptime T: type, body: []const u8) !std.json.Parsed(T) {
    return std.json.parseFromSlice(T, std.heap.page_allocator, body, .{});
}

fn parseJsonBody(comptime T: type, body: []const u8) !std.json.Parsed(T) {
    return std.json.parseFromSlice(T, std.heap.page_allocator, body, .{});
}

fn parseJsonBodyIgnoreUnknown(comptime T: type, body: []const u8) !std.json.Parsed(T) {
    return std.json.parseFromSlice(T, std.heap.page_allocator, body, .{ .ignore_unknown_fields = true });
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

const LookupTitle = struct {
    title: []const u8,
};

const UserName = struct {
    name: []const u8,
};

const OrderItem = struct {
    item: []const u8,
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

        var parsed_req = try parseJsonBodyIgnoreUnknown(TestEmbeddingRequest, req.body);
        defer parsed_req.deinit();

        const vector = if (jsonValueContainsText(parsed_req.value.input, "alpha concept") or jsonValueContainsText(parsed_req.value.input, "alpha body") or jsonValueContainsText(parsed_req.value.input, "left alpha body"))
            "[1,0,0]"
        else if (jsonValueContainsText(parsed_req.value.input, "beta body") or jsonValueContainsText(parsed_req.value.input, "right beta body"))
            "[0,1,0]"
        else
            "[0,0,1]";

        const body = try std.fmt.allocPrint(
            alloc,
            "{{\"object\":\"list\",\"data\":[{{\"object\":\"embedding\",\"index\":0,\"embedding\":{s}}}],\"model\":\"test-embed\",\"usage\":{{\"prompt_tokens\":1,\"total_tokens\":1}}}}",
            .{vector},
        );

        return .{
            .status = 200,
            .content_type = try alloc.dupe(u8, "application/json"),
            .body = body,
        };
    }
};

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
            var parsed_req = try parseJsonBodyIgnoreUnknown(TestAntflyChunkRequest, req.body);
            defer parsed_req.deinit();
            try std.testing.expectEqualStrings("antfly-chunker-v1", parsed_req.value.config.model);
            const body = if (jsonValueContainsText(parsed_req.value.input, "beta body") or jsonValueContainsText(parsed_req.value.input, "right beta body"))
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
            var parsed_req = try parseJsonBodyIgnoreUnknown(TestAntflyEmbedRequest, req.body);
            defer parsed_req.deinit();
            if (std.mem.eql(u8, parsed_req.value.model, "antfly-sparse-v1")) {
                const body = if (jsonValueContainsText(parsed_req.value.input, "alpha body") or jsonValueContainsText(parsed_req.value.input, "left alpha body"))
                    try encodeSparseEmbeddingResponse(alloc, &.{ 7, 42 }, &.{ 1.5, 0.5 })
                else if (jsonValueContainsText(parsed_req.value.input, "beta body") or jsonValueContainsText(parsed_req.value.input, "right beta body"))
                    try encodeSparseEmbeddingResponse(alloc, &.{ 7, 42 }, &.{ 0.25, 1.0 })
                else
                    try encodeSparseEmbeddingResponse(alloc, &.{99}, &.{0.1});
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "application/json"),
                    .body = body,
                };
            }

            const vector: [3]f32 = if (jsonValueContainsText(parsed_req.value.input, "alpha concept") or jsonValueContainsText(parsed_req.value.input, "alpha body") or jsonValueContainsText(parsed_req.value.input, "left alpha body"))
                .{ 1, 0, 0 }
            else if (jsonValueContainsText(parsed_req.value.input, "image/png") or jsonValueContainsText(parsed_req.value.input, "media"))
                .{ 1, 0, 0 }
            else if (jsonValueContainsText(parsed_req.value.input, "beta body") or jsonValueContainsText(parsed_req.value.input, "right beta body"))
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

        return error.TestUnexpectedResult;
    }
};

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
        if (std.mem.endsWith(u8, req.uri, "/kitten.png")) {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "image/png"),
                .body = try alloc.dupe(u8, "png-bytes"),
            };
        }
        if (std.mem.endsWith(u8, req.uri, "/doc.txt")) {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "alpha concept"),
            };
        }
        if (std.mem.endsWith(u8, req.uri, "/missing.pdf")) {
            return .{
                .status = 404,
                .content_type = try alloc.dupe(u8, "application/pdf"),
                .body = try alloc.dupe(u8, ""),
            };
        }
        return error.TestUnexpectedResult;
    }
};

const Factory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,
    peers: []const raft_engine.core.types.NodeId,
    group_stores: std.AutoHashMapUnmanaged(u64, *raft_engine.core.MemoryStorage) = .empty,
    primary_group_id: ?u64 = null,

    fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn storageForGroup(self: *@This(), group_id: u64) !*raft_engine.core.MemoryStorage {
        if (self.group_stores.get(group_id)) |store| return store;
        if (self.primary_group_id == null) {
            self.primary_group_id = group_id;
            try self.group_stores.put(std.heap.page_allocator, group_id, self.store);
            return self.store;
        }
        const store = try std.heap.page_allocator.create(raft_engine.core.MemoryStorage);
        errdefer std.heap.page_allocator.destroy(store);
        store.* = raft_engine.core.MemoryStorage.init(std.heap.page_allocator);
        errdefer store.deinit();
        try self.group_stores.put(std.heap.page_allocator, group_id, store);
        return store;
    }

    fn buildDescriptor(ptr: *anyopaque, record: raft_catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const store = try self.storageForGroup(record.group_id);
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, self.peers);
        errdefer self.alloc.free(peers);
        var bootstrap = try raft_catalog.runtimeBootstrapFromRecord(self.alloc, record);
        errdefer raft_catalog.freeRuntimeBootstrap(self.alloc, &bootstrap);
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
                .storage = store.storage(),
            },
            .bootstrap = bootstrap,
        };
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        raft_catalog.freeRuntimeBootstrap(alloc, &desc.bootstrap);
        self.alloc.free(desc.group.raft_config.peers);
    }
};

fn makeHostSimConfig(
    local_node_id: u64,
    metadata_group_id: u64,
    replica_root_dir: []const u8,
    replica_catalog_path: []const u8,
) raft_sim.ManagedHttpHostSimulationConfig {
    return .{
        .host = .{
            .http = .{
                .host = .{
                    .local_node_id = local_node_id,
                    .metadata_group_id = metadata_group_id,
                    .replica_root_dir = replica_root_dir,
                    .replica_catalog_path = replica_catalog_path,
                },
                .transport = .{
                    .snapshot = .{ .root_dir = replica_root_dir },
                },
            },
        },
    };
}

fn makeHostSimDeps(factory: *Factory) raft_sim.ManagedHttpHostSimulationDeps {
    return .{
        .host = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory.iface(),
                },
            },
        },
    };
}

const PublicApiStatusSource = struct {
    node: metadata_sim.MetadataHttpNodeSimulation,

    fn iface(self: *@This()) api_http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .create_table = createTable,
                .drop_table = dropTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .drop_index = dropIndex,
            },
        };
    }

    fn status(ptr: *anyopaque) !metadata_service.MetadataStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.node.metadataStatus();
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.node.adminSnapshot();
    }

    fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.node.freeAdminSnapshot(snapshot);
    }

    fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: api_tables.CreateTableRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        const table = api_tables.deriveTableRecord(table_name, req);
        _ = try workflow.createTable(&self.node, table, api_tables.deriveInitialRange(table));
    }

    fn dropTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var snapshot = try self.node.adminSnapshot();
        defer self.node.freeAdminSnapshot(&snapshot);
        const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        _ = try workflow.dropTable(&self.node, table.table_id);
    }

    fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var snapshot = try self.node.adminSnapshot();
        defer self.node.freeAdminSnapshot(&snapshot);
        const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        const updated = try api_tables.applySchemaUpdateRecord(alloc, table, schema_json);
        defer metadata_table_manager.freeTable(alloc, updated);
        try self.node.upsertTable(updated);
    }

    fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var snapshot = try self.node.adminSnapshot();
        defer self.node.freeAdminSnapshot(&snapshot);
        const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        var updated = table.*;
        updated.indexes_json = try indexes_api.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, index_json);
        defer alloc.free(updated.indexes_json);
        try self.node.upsertTable(updated);
    }

    fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var snapshot = try self.node.adminSnapshot();
        defer self.node.freeAdminSnapshot(&snapshot);
        const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        const next = (try indexes_api.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
        defer alloc.free(next);
        var updated = table.*;
        updated.indexes_json = next;
        try self.node.upsertTable(updated);
    }
};

const PublicApiCatalogSource = struct {
    node: metadata_sim.MetadataHttpNodeSimulation,

    fn iface(self: *@This()) api_table_catalog.CatalogSource {
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
        return try self.node.adminSnapshot();
    }

    fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.node.freeAdminSnapshot(snapshot);
    }
};

fn PublicApiRouter(comptime N: usize) type {
    return struct {
        node: metadata_sim.MetadataHttpNodeSimulation,
        cluster: *metadata_sim.MetadataHttpClusterSimulation,
        api_base_uris: *const [N][]const u8,

        fn iface(self: *@This()) api_table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @as(u64, @intCast(self.node.index + 1));
        }

        fn localStatus(ptr: *anyopaque, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.node.status(group_id);
        }

        fn groupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (self.cluster.cluster.nodes) |*sim| {
                if (sim.leaderId(group_id)) |leader_id| return leader_id;
            }
            return null;
        }

        fn nodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.cluster.cluster.nodes.len) return .absent;
            return self.cluster.node(@intCast(node_id - 1)).status(group_id);
        }

        fn nodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.api_base_uris.len) return null;
            return try alloc.dupe(u8, self.api_base_uris[node_id - 1]);
        }
    };
}

fn startPublicApiServers(
    comptime N: usize,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    roots: *const [N][]const u8,
    forward_executor: *std_http_executor.StdHttpExecutor,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]api_http_server.ApiHttpServer,
    status_sources: *[N]PublicApiStatusSource,
    catalog_sources: *[N]PublicApiCatalogSource,
    routers: *[N]PublicApiRouter(N),
    read_sources: *[N]api_table_reads.HostedProvisionedTableReadSource,
    write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource,
    api_base_uris: *[N][]const u8,
) !void {
    return try startPublicApiServersWithOptionalSessions(
        N,
        cluster,
        roots,
        forward_executor.executor(),
        forward_executor.io_impl,
        null,
        listeners,
        servers,
        status_sources,
        catalog_sources,
        routers,
        read_sources,
        write_sources,
        api_base_uris,
    );
}

fn startPublicApiServersWithExecutor(
    comptime N: usize,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    roots: *const [N][]const u8,
    forward_executor: http_common.RequestExecutor,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]api_http_server.ApiHttpServer,
    status_sources: *[N]PublicApiStatusSource,
    catalog_sources: *[N]PublicApiCatalogSource,
    routers: *[N]PublicApiRouter(N),
    read_sources: *[N]api_table_reads.HostedProvisionedTableReadSource,
    write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource,
    api_base_uris: *[N][]const u8,
) !void {
    return try startPublicApiServersWithOptionalSessions(
        N,
        cluster,
        roots,
        forward_executor,
        null,
        null,
        listeners,
        servers,
        status_sources,
        catalog_sources,
        routers,
        read_sources,
        write_sources,
        api_base_uris,
    );
}

fn startPublicApiServersWithDurableSessions(
    comptime N: usize,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    roots: *const [N][]const u8,
    forward_executor: http_common.RequestExecutor,
    session_stores: *const [N]?*transactions_api.DurableSessionStore,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]api_http_server.ApiHttpServer,
    status_sources: *[N]PublicApiStatusSource,
    catalog_sources: *[N]PublicApiCatalogSource,
    routers: *[N]PublicApiRouter(N),
    read_sources: *[N]api_table_reads.HostedProvisionedTableReadSource,
    write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource,
    api_base_uris: *[N][]const u8,
) !void {
    return try startPublicApiServersWithOptionalSessions(
        N,
        cluster,
        roots,
        forward_executor,
        null,
        session_stores,
        listeners,
        servers,
        status_sources,
        catalog_sources,
        routers,
        read_sources,
        write_sources,
        api_base_uris,
    );
}

fn startPublicApiServersWithSharedSessionStorePath(
    comptime N: usize,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    roots: *const [N][]const u8,
    forward_executor: *std_http_executor.StdHttpExecutor,
    session_store_path: []const u8,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]api_http_server.ApiHttpServer,
    status_sources: *[N]PublicApiStatusSource,
    catalog_sources: *[N]PublicApiCatalogSource,
    routers: *[N]PublicApiRouter(N),
    read_sources: *[N]api_table_reads.HostedProvisionedTableReadSource,
    write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource,
    api_base_uris: *[N][]const u8,
) !void {
    for (0..N) |i| {
        status_sources[i] = .{ .node = cluster.node(i) };
        catalog_sources[i] = .{ .node = cluster.node(i) };
        routers[i] = .{ .node = cluster.node(i), .cluster = cluster, .api_base_uris = api_base_uris };
        read_sources[i] = api_table_reads.HostedProvisionedTableReadSource.init(
            roots[i],
            catalog_sources[i].iface(),
            cluster.cluster.node(i).runtime.svc.readableLeaseRequester(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        _ = read_sources[i].withIo(forward_executor.io_impl);
        _ = read_sources[i].withBackendRuntime(cluster.backendRuntime(i));
        write_sources[i] = api_table_writes.HostedProvisionedTableWriteSource.init(
            roots[i],
            catalog_sources[i].iface(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        _ = write_sources[i].withBackendRuntime(cluster.backendRuntime(i));
        servers[i] = try api_http_server.ApiHttpServer.initWithConfig(
            std.testing.allocator,
            .{
                .session_router = routers[i].iface(),
                .session_executor = forward_executor.executor(),
                .session_store_path = session_store_path,
                .session_owner_lease_ttl_ns = std.time.ns_per_ms,
            },
            status_sources[i].iface(),
            read_sources[i].source(),
            write_sources[i].source(),
        );
        listeners[i] = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, servers[i].executor());
        try listeners[i].start();
    }
    for (0..N) |i| api_base_uris[i] = try listeners[i].baseUri(std.testing.allocator);
}

fn startPublicApiServersWithOptionalSessions(
    comptime N: usize,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    roots: *const [N][]const u8,
    forward_executor: http_common.RequestExecutor,
    forward_io_impl: ?*std.Io.Threaded,
    session_stores: ?*const [N]?*transactions_api.DurableSessionStore,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]api_http_server.ApiHttpServer,
    status_sources: *[N]PublicApiStatusSource,
    catalog_sources: *[N]PublicApiCatalogSource,
    routers: *[N]PublicApiRouter(N),
    read_sources: *[N]api_table_reads.HostedProvisionedTableReadSource,
    write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource,
    api_base_uris: *[N][]const u8,
) !void {
    for (0..N) |i| {
        status_sources[i] = .{ .node = cluster.node(i) };
        catalog_sources[i] = .{ .node = cluster.node(i) };
        routers[i] = .{ .node = cluster.node(i), .cluster = cluster, .api_base_uris = api_base_uris };
        read_sources[i] = api_table_reads.HostedProvisionedTableReadSource.init(
            roots[i],
            catalog_sources[i].iface(),
            cluster.cluster.node(i).runtime.svc.readableLeaseRequester(),
            routers[i].iface(),
            forward_executor,
        );
        if (forward_io_impl) |io_impl| _ = read_sources[i].withIo(io_impl);
        _ = read_sources[i].withBackendRuntime(cluster.backendRuntime(i));
        write_sources[i] = api_table_writes.HostedProvisionedTableWriteSource.init(
            roots[i],
            catalog_sources[i].iface(),
            routers[i].iface(),
            forward_executor,
        );
        _ = write_sources[i].withBackendRuntime(cluster.backendRuntime(i));
        servers[i] = api_http_server.ApiHttpServer.init(
            std.testing.allocator,
            .{
                .session_router = routers[i].iface(),
                .session_executor = forward_executor,
                .session_store = if (session_stores) |stores| stores[i] else null,
            },
            status_sources[i].iface(),
            read_sources[i].source(),
            write_sources[i].source(),
        );
        listeners[i] = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, servers[i].executor());
        try listeners[i].start();
    }
    for (0..N) |i| api_base_uris[i] = try listeners[i].baseUri(std.testing.allocator);
}

const GraphChurnMode = enum {
    merge_once,
    merge_then_split,
};

const GraphTopologyChurnExecutor = struct {
    forward: http_common.RequestExecutor,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    metadata_apis: *const [4][]const u8,
    mode: GraphChurnMode,
    trigger_count: u32 = 0,

    fn executor(self: *@This()) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (req.method == .POST and std.mem.indexOf(u8, req.uri, api_routes.Routes.graph_expand_suffix) != null) {
            switch (self.mode) {
                .merge_once => if (self.trigger_count == 0) {
                    try injectDocsMerge(alloc, self.forward, self.cluster, self.metadata_apis, 990001);
                    self.trigger_count += 1;
                },
                .merge_then_split => if (self.trigger_count == 0) {
                    try injectDocsMerge(alloc, self.forward, self.cluster, self.metadata_apis, 990001);
                    self.trigger_count += 1;
                } else if (self.trigger_count == 1) {
                    try injectDocsSplit(alloc, self.forward, self.cluster, self.metadata_apis, 990002, "doc:m");
                    self.trigger_count += 1;
                },
            }
        }
        return try self.forward.execute(alloc, req);
    }
};

const TxnChurnMode = enum {
    merge_once,
    merge_then_split,
};

const TxnTopologyChurnExecutor = struct {
    forward: http_common.RequestExecutor,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    metadata_apis: *const [4][]const u8,
    mode: TxnChurnMode,
    trigger_count: u32 = 0,

    fn executor(self: *@This()) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (req.method == .POST and std.mem.indexOf(u8, req.uri, api_routes.Routes.txn_prepare_suffix) != null) {
            switch (self.mode) {
                .merge_once => if (self.trigger_count == 0) {
                    try injectDocsMerge(alloc, self.forward, self.cluster, self.metadata_apis, 991001);
                    self.trigger_count += 1;
                },
                .merge_then_split => if (self.trigger_count == 0) {
                    try injectDocsMerge(alloc, self.forward, self.cluster, self.metadata_apis, 991001);
                    self.trigger_count += 1;
                } else if (self.trigger_count == 1) {
                    try injectDocsSplit(alloc, self.forward, self.cluster, self.metadata_apis, 991002, "doc:m");
                    self.trigger_count += 1;
                },
            }
        }
        return try self.forward.execute(alloc, req);
    }
};

fn injectDocsMerge(
    alloc: std.mem.Allocator,
    forward: http_common.RequestExecutor,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    metadata_apis: *const [4][]const u8,
    transition_id: u64,
) !void {
    var metadata_client = metadata_http_client.MetadataHttpClient.init(alloc, forward);
    const leader_index = currentMetadataLeaderIndex(cluster) orelse return error.TestExpectedEqual;
    var snapshot = try cluster.node(leader_index).adminSnapshot();
    defer cluster.node(leader_index).freeAdminSnapshot(&snapshot);
    const table = findAdminTableByName(&snapshot, "docs") orelse return error.TableNotFound;
    const left_group = findRangeForKey(snapshot.ranges, table.table_id, "doc:a") orelse return error.RangeNotFound;
    const right_group = findRangeForKey(snapshot.ranges, table.table_id, "doc:z") orelse return error.RangeNotFound;
    if (left_group == right_group) return;

    const body = try std.fmt.allocPrint(alloc, "{{\"transition_id\":{d},\"donor_group_id\":{d},\"receiver_group_id\":{d}}}", .{
        transition_id,
        right_group,
        left_group,
    });
    defer alloc.free(body);
    try metadata_client.requestTableMerge(metadata_apis[currentMetadataLeaderIndex(cluster) orelse leader_index], "docs", body);

    var finalized = false;
    var rounds: usize = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(cluster) orelse leader_index;
        if (try cluster.node(query_index).observeMergeTransition(transition_id)) |observation| {
            if (observation.receiver.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(cluster) orelse leader_index]);
    try cluster.stepAll();
}

fn injectDocsSplit(
    alloc: std.mem.Allocator,
    forward: http_common.RequestExecutor,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    metadata_apis: *const [4][]const u8,
    transition_id: u64,
    split_key: []const u8,
) !void {
    var metadata_client = metadata_http_client.MetadataHttpClient.init(alloc, forward);
    const leader_index = currentMetadataLeaderIndex(cluster) orelse return error.TestExpectedEqual;
    var snapshot = try cluster.node(leader_index).adminSnapshot();
    defer cluster.node(leader_index).freeAdminSnapshot(&snapshot);
    const table = findAdminTableByName(&snapshot, "docs") orelse return error.TableNotFound;
    const source_group = findRangeForKey(snapshot.ranges, table.table_id, "doc:a") orelse return error.RangeNotFound;

    const body = try std.fmt.allocPrint(alloc, "{{\"transition_id\":{d},\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":{f}}}", .{
        transition_id,
        source_group,
        source_group + 1000,
        std.json.fmt(split_key, .{}),
    });
    defer alloc.free(body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(cluster) orelse leader_index], "docs", body);

    var finalized = false;
    var rounds: usize = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(transition_id)) |observation| {
            if (observation.status.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(cluster) orelse leader_index]);
    try cluster.stepAll();
}

fn currentMetadataLeaderIndex(cluster: *metadata_sim.MetadataHttpClusterSimulation) ?usize {
    for (cluster.cluster.nodes, 0..) |*sim, index| {
        if (sim.raftStatus(cluster.metadata_group_id)) |status| {
            if (status.soft.role == .leader) return index;
        }
    }
    return null;
}

fn currentGroupLeaderIndex(cluster: *metadata_sim.MetadataHttpClusterSimulation, group_id: u64) ?usize {
    for (cluster.cluster.nodes, 0..) |*sim, index| {
        if (sim.leaderId(group_id)) |leader_id| {
            if (leader_id == cluster.cluster.configs[index].host.http.host.local_node_id) return index;
        }
    }
    return null;
}

fn seedHalfResolvedCommittedTxnOnGroup(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    participants: []const []const u8,
    local_participant: []const u8,
    write_key: []const u8,
    write_value: []const u8,
) !void {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    _ = try db.beginTransactionWithIdAndParticipants(txn_id, begin_timestamp, participants);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = write_key, .value = write_value }},
    });
    try db.resolveTransactionIntents(txn_id, .committed, commit_version);
    try db.markTransactionParticipantResolved(txn_id, local_participant);
}

fn expectTxnCleanedOnGroup(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    txn_id: db_mod.types.TxnId,
) !void {
    const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
}

fn findAdminTableByName(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?*const metadata_table_manager.TableRecord {
    for (snapshot.tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn findRangeForKey(records: []const metadata_table_manager.RangeRecord, table_id: u64, key: []const u8) ?u64 {
    for (records) |record| {
        if (record.table_id != table_id) continue;
        if (key.len > 0 and record.start_key.len > 0 and std.mem.order(u8, key, record.start_key) == .lt) continue;
        if (record.end_key) |end_key| {
            if (std.mem.order(u8, key, end_key) != .lt) continue;
        }
        return record.group_id;
    }
    return null;
}

fn deriveTransitionId(table_name: []const u8, key: []const u8, seed: u64) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(key);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

fn deriveGroupId(table_name: []const u8, key: []const u8, seed: u64, reserved: u64) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(key);
    var id = hasher.final();
    if (id == 0 or id == reserved) id +%= 1;
    if (id == 0) return reserved +% 1;
    return id;
}

fn ensureGroupTextIndex(
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    replica_root_dir: []const u8,
    group_id: u64,
    index_name: []const u8,
    max_rounds: usize,
) !void {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root_dir, group_id);
        defer std.testing.allocator.free(path);

        var db = db_mod.DB.open(std.testing.allocator, path, .{}) catch |err| switch (err) {
            error.PathAlreadyExists, error.FileNotFound => {
                try cluster.stepAll();
                continue;
            },
            else => return err,
        };
        defer db.close();

        if (db.core.index_manager.textIndex(index_name) == null) {
            try db.addIndex(.{
                .name = index_name,
                .kind = .full_text,
                .config_json = "{}",
            });
        }
        return;
    }
    return error.FileNotFound;
}

fn ensureGroupEmbeddingIndexes(
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    replica_root_dir: []const u8,
    group_id: u64,
    dense_index_name: []const u8,
    sparse_index_name: []const u8,
    max_rounds: usize,
) !void {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root_dir, group_id);
        defer std.testing.allocator.free(path);

        var db = db_mod.DB.open(std.testing.allocator, path, .{}) catch |err| switch (err) {
            error.PathAlreadyExists, error.FileNotFound => {
                try cluster.stepAll();
                continue;
            },
            else => return err,
        };
        defer db.close();

        if (db.core.index_manager.denseIndex(dense_index_name) == null or db.core.index_manager.sparseIndex(sparse_index_name) == null) {
            try cluster.stepAll();
            continue;
        }
        return;
    }
    return error.FileNotFound;
}

fn ensureGroupDenseIndex(
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    replica_root_dir: []const u8,
    group_id: u64,
    dense_index_name: []const u8,
    max_rounds: usize,
) !void {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root_dir, group_id);
        defer std.testing.allocator.free(path);

        var db = db_mod.DB.open(std.testing.allocator, path, .{}) catch |err| switch (err) {
            error.PathAlreadyExists, error.FileNotFound => {
                try cluster.stepAll();
                continue;
            },
            else => return err,
        };
        defer db.close();

        if (db.core.index_manager.denseIndex(dense_index_name) == null) {
            try cluster.stepAll();
            continue;
        }
        return;
    }
    return error.FileNotFound;
}

fn ensureGroupGraphIndex(
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    replica_root_dir: []const u8,
    group_id: u64,
    index_name: []const u8,
    max_rounds: usize,
) !void {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(std.testing.allocator, replica_root_dir, group_id);
        defer std.testing.allocator.free(path);

        var db = db_mod.DB.open(std.testing.allocator, path, .{}) catch |err| switch (err) {
            error.PathAlreadyExists, error.FileNotFound => {
                try cluster.stepAll();
                continue;
            },
            else => return err,
        };
        defer db.close();

        if (db.core.index_manager.graphIndex(index_name) == null) {
            try cluster.stepAll();
            continue;
        }
        return;
    }
    return error.FileNotFound;
}

fn expectGraphNodeKeys(
    nodes: ?[]const indexes_openapi.GraphResultNode,
    expected: []const []const u8,
) !void {
    const actual = nodes orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(expected.len, actual.len);

    var matched = try std.testing.allocator.alloc(bool, actual.len);
    defer std.testing.allocator.free(matched);
    @memset(matched, false);

    for (expected) |key| {
        var found = false;
        for (actual, 0..) |node, i| {
            if (matched[i]) continue;
            if (!std.mem.eql(u8, node.key, key)) continue;
            matched[i] = true;
            found = true;
            break;
        }
        try std.testing.expect(found);
    }
}

fn expectQueryProfileSummary(
    alloc: std.mem.Allocator,
    profile: ?std.json.Value,
    expected_shards_total: i64,
    expect_merge: bool,
) !void {
    const profile_value = profile orelse return error.TestExpectedEqual;
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(profile_value, .{})});
    defer alloc.free(encoded);

    const Summary = struct {
        shards: struct {
            total: i64,
        },
        merge: ?std.json.Value = null,
    };

    var parsed = try std.json.parseFromSlice(Summary, alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(expected_shards_total, parsed.value.shards.total);
    try std.testing.expectEqual(expect_merge, parsed.value.merge != null);
}

fn findGraphNode(
    nodes: ?[]const indexes_openapi.GraphResultNode,
    key: []const u8,
) ?indexes_openapi.GraphResultNode {
    const actual = nodes orelse return null;
    for (actual) |node| {
        if (std.mem.eql(u8, node.key, key)) return node;
    }
    return null;
}

fn expectGraphNodePath(
    nodes: ?[]const indexes_openapi.GraphResultNode,
    key: []const u8,
    expected_path: []const []const u8,
    expected_edge_types: []const []const u8,
) !void {
    const node = findGraphNode(nodes, key) orelse return error.TestExpectedEqual;
    const path = node.path orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(expected_path.len, path.len);
    for (expected_path, path) |expected, actual| {
        try std.testing.expectEqualStrings(expected, actual);
    }

    const path_edges = node.path_edges orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(expected_edge_types.len, path_edges.len);
    for (expected_edge_types, path_edges) |expected, actual| {
        try std.testing.expectEqualStrings(expected, actual.type.?);
    }
}

const MetadataAdminSimSource = struct {
    node: metadata_sim.MetadataHttpNodeSimulation,

    fn iface(self: *@This()) metadata_http_server.AdminSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .trigger_reallocate = triggerReallocate,
                .request_split = requestSplit,
                .request_merge = requestMerge,
            },
        };
    }

    fn status(ptr: *anyopaque) !metadata_service.MetadataStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.node.metadataStatus();
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.node.adminSnapshot();
    }

    fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.node.freeAdminSnapshot(snapshot);
    }

    fn triggerReallocate(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.node.runRound();
    }

    fn requestSplit(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: metadata_http_server.SplitRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var snapshot = try self.node.adminSnapshot();
        defer self.node.freeAdminSnapshot(&snapshot);
        const table = findAdminTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        const source_group_id = req.source_group_id orelse findRangeForKey(snapshot.ranges, table.table_id, req.split_key) orelse return error.RangeNotFound;

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        try workflow.bootstrapDesiredFromCommitted(&self.node);
        _ = try workflow.requestSplit(&self.node, .{
            .transition_id = req.transition_id orelse deriveTransitionId(table_name, req.split_key, 0x53504c54),
            .table_id = table.table_id,
            .source_group_id = source_group_id,
            .destination_group_id = req.destination_group_id orelse deriveGroupId(table_name, req.split_key, 0x53504c47, source_group_id),
            .split_key = req.split_key,
        });
        try self.node.runRound();
    }

    fn requestMerge(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: metadata_http_server.MergeRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var snapshot = try self.node.adminSnapshot();
        defer self.node.freeAdminSnapshot(&snapshot);
        const table = findAdminTableByName(&snapshot, table_name) orelse return error.TableNotFound;

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        try workflow.bootstrapDesiredFromCommitted(&self.node);
        _ = try workflow.requestMerge(&self.node, .{
            .transition_id = req.transition_id orelse deriveTransitionId(table_name, table_name, 0x4d524754),
            .table_id = table.table_id,
            .donor_group_id = req.donor_group_id,
            .receiver_group_id = req.receiver_group_id,
            .allow_doc_identity_reassignment = req.allow_doc_identity_reassignment,
        });
        try self.node.runRound();
    }
};

fn startMetadataAdminServers(
    comptime N: usize,
    cluster: *metadata_sim.MetadataHttpClusterSimulation,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]metadata_http_server.MetadataHttpServer,
    sources: *[N]MetadataAdminSimSource,
    base_uris: *[N][]const u8,
) !void {
    for (0..N) |i| {
        sources[i] = .{ .node = cluster.node(i) };
        servers[i] = metadata_http_server.MetadataHttpServer.init(std.testing.allocator, .{}, sources[i].iface());
        listeners[i] = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, servers[i].executor());
        try listeners[i].start();
    }
    for (0..N) |i| base_uris[i] = try listeners[i].baseUri(std.testing.allocator);
}

test "public api multi-node e2e routes CRUD from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6110, root_a, cat_a),
        makeHostSimConfig(2, 6110, root_b, cat_b),
        makeHostSimConfig(3, 6110, root_c, cat_c),
        makeHostSimConfig(4, 6110, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6110, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node docs");
    defer std.heap.page_allocator.free(create_body);
    var created = try client.createTable(api_base_uris[0], "docs", create_body);
    defer created.deinit(std.heap.page_allocator);
    var created_table = try std.json.parseFromSlice(metadata_openapi.Table, std.heap.page_allocator, created.body, .{});
    defer created_table.deinit();
    try std.testing.expectEqualStrings("docs", created_table.value.name);

    var group_id: u64 = 0;
    var non_host_index: ?usize = null;
    var active_count: usize = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = 0;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        group_id = projected_ranges[0].group_id;

        active_count = 0;
        non_host_index = null;
        for (0..4) |i| {
            if (cluster.node(i).status(group_id) == .active) {
                active_count += 1;
            } else {
                non_host_index = i;
            }
        }
        if (active_count == 3 and non_host_index != null) break;
    }
    try std.testing.expectEqual(@as(usize, 3), active_count);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(std.heap.page_allocator);
    defer std.heap.page_allocator.free(schema_body);
    var updated_schema = try client.updateTableSchema(client_base, "docs", schema_body);
    defer updated_schema.deinit(std.heap.page_allocator);
    var parsed_updated_schema = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.heap.page_allocator, updated_schema.body, .{});
    defer parsed_updated_schema.deinit();
    try std.testing.expect(parsed_updated_schema.value.schema != null);
    try std.testing.expect(parsed_updated_schema.value.schema.?.document_schemas != null);
    try std.testing.expect(parsed_updated_schema.value.migration != null);
    try std.testing.expectEqualStrings("rebuilding", parsed_updated_schema.value.migration.?.state);
    try std.testing.expectEqual(@as(?i64, 0), parsed_updated_schema.value.migration.?.read_schema.version);

    var table_detail_after_schema = try client.fetchTable(client_base, "docs");
    defer table_detail_after_schema.deinit(std.heap.page_allocator);
    var parsed_table_detail_after_schema = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.heap.page_allocator, table_detail_after_schema.body, .{});
    defer parsed_table_detail_after_schema.deinit();
    try std.testing.expect(parsed_table_detail_after_schema.value.migration != null);
    try std.testing.expectEqualStrings("rebuilding", parsed_table_detail_after_schema.value.migration.?.state);
    try std.testing.expect(parsed_table_detail_after_schema.value.indexes.map.get("full_text_index_v0") != null);
    try std.testing.expect(parsed_table_detail_after_schema.value.indexes.map.get("full_text_index_v1") != null);

    var indexes_after_schema = try client.fetchTableIndexes(client_base, "docs");
    defer indexes_after_schema.deinit(std.heap.page_allocator);
    var parsed_indexes_after_schema = try std.json.parseFromSlice([]metadata_openapi.IndexStatus, std.heap.page_allocator, indexes_after_schema.body, .{});
    defer parsed_indexes_after_schema.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_indexes_after_schema.value.len);
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_indexes_after_schema.value[0].config.name);
    try std.testing.expectEqualStrings("full_text_index_v1", parsed_indexes_after_schema.value[1].config.name);

    const index_body = try test_contract_helpers.encodeCreateIndexRequest(std.heap.page_allocator, "embed_idx");
    defer std.heap.page_allocator.free(index_body);
    var created_index = try client.createTableIndex(client_base, "docs", "embed_idx", index_body);
    defer created_index.deinit(std.heap.page_allocator);

    var indexes = try client.fetchTableIndexes(client_base, "docs");
    defer indexes.deinit(std.heap.page_allocator);
    var parsed_indexes = try std.json.parseFromSlice([]metadata_openapi.IndexStatus, std.heap.page_allocator, indexes.body, .{});
    defer parsed_indexes.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed_indexes.value.len);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello remote world"},"doc:b":{"title":"beta","body":"secondary remote document"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(client_base, "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_batch.value.inserted.?);

    var lookup = try client.fetchLookup(client_base, "docs", "doc:a", null);
    defer lookup.deinit(std.heap.page_allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("alpha", parsed_lookup.value.title);

    const query_body = try test_contract_helpers.encodeMatchQueryRequestWithFlags(
        std.heap.page_allocator,
        "body",
        "remote",
        &.{ "title", "body" },
        10,
        true,
        true,
    );
    defer std.heap.page_allocator.free(query_body);
    var query = try client.fetchQuery(client_base, "docs", query_body);
    defer query.deinit(std.heap.page_allocator);
    var query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, query.body, .{});
    defer query_responses.deinit();
    const query_result = query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 2), query_result.hits.?.total.?);
    try std.testing.expect(query_result.profile != null);

    const delete_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator, "{\"deletes\":[\"doc:a\"]}");
    defer std.heap.page_allocator.free(delete_body);
    var deleted = try client.fetchBatch(client_base, "docs", delete_body);
    defer deleted.deinit(std.heap.page_allocator);
    var parsed_deleted = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, deleted.body, .{});
    defer parsed_deleted.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_deleted.value.deleted.?);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:a", null));

    var dropped_index = try client.deleteTableIndex(client_base, "docs", "embed_idx");
    defer dropped_index.deinit(std.heap.page_allocator);

    var indexes_after_drop = try client.fetchTableIndexes(client_base, "docs");
    defer indexes_after_drop.deinit(std.heap.page_allocator);
    var parsed_indexes_after_drop = try std.json.parseFromSlice([]metadata_openapi.IndexStatus, std.heap.page_allocator, indexes_after_drop.body, .{});
    defer parsed_indexes_after_drop.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_indexes_after_drop.value.len);
    try std.testing.expectEqualStrings("full_text_index_v1", parsed_indexes_after_drop.value[0].config.name);
    try std.testing.expectEqualStrings("embed_idx", parsed_indexes_after_drop.value[1].config.name);

    var stable_table_detail = try client.fetchTable(client_base, "docs");
    defer stable_table_detail.deinit(std.heap.page_allocator);
    var parsed_stable_table_detail = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.heap.page_allocator, stable_table_detail.body, .{});
    defer parsed_stable_table_detail.deinit();
    try std.testing.expect(parsed_stable_table_detail.value.migration == null);
    try std.testing.expect(parsed_stable_table_detail.value.indexes.map.get("full_text_index_v0") == null);
    try std.testing.expect(parsed_stable_table_detail.value.indexes.map.get("full_text_index_v1") != null);
}

test "public api multi-node e2e routes transaction commit from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6145, root_a, cat_a),
        makeHostSimConfig(2, 6145, root_b, cat_b),
        makeHostSimConfig(3, 6145, root_c, cat_c),
        makeHostSimConfig(4, 6145, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6145, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node txn docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var group_id: u64 = 0;
    var non_host_index: ?usize = null;
    var active_count: usize = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        group_id = projected_ranges[0].group_id;
        active_count = 0;
        non_host_index = null;
        for (0..4) |i| {
            if (cluster.node(i).status(group_id) == .active) {
                active_count += 1;
            } else {
                non_host_index = i;
            }
        }
        if (active_count == 3 and non_host_index != null) break;
    }
    try std.testing.expectEqual(@as(usize, 3), active_count);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello txn"},"doc:b":{"title":"beta","body":"hello txn two"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(client_base, "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var left = try client.fetchLookup(client_base, "docs", "doc:a", null);
    defer left.deinit(std.heap.page_allocator);
    var right = try client.fetchLookup(client_base, "docs", "doc:b", null);
    defer right.deinit(std.heap.page_allocator);

    const commit_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha committed","body":"txn committed a"},"doc:b":{"title":"beta committed","body":"txn committed b"}}}
    );
    defer std.heap.page_allocator.free(commit_batch);
    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{
            .{ .table_name = "docs", .key = "doc:a", .version = left.version.? },
            .{ .table_name = "docs", .key = "doc:b", .version = right.version.? },
        },
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        null,
    );
    defer std.heap.page_allocator.free(commit_body);

    var committed = try client.fetchTransactionCommit(client_base, commit_body);
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.heap.page_allocator, committed.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);

    var updated_a = try client.fetchLookup(client_base, "docs", "doc:a", null);
    defer updated_a.deinit(std.heap.page_allocator);
    var parsed_updated_a = try parseJsonBody(LookupTitle, updated_a.body);
    defer parsed_updated_a.deinit();
    try std.testing.expectEqualStrings("alpha committed", parsed_updated_a.value.title);
    var updated_b = try client.fetchLookup(client_base, "docs", "doc:b", null);
    defer updated_b.deinit(std.heap.page_allocator);
    var parsed_updated_b = try parseJsonBody(LookupTitle, updated_b.body);
    defer parsed_updated_b.deinit();
    try std.testing.expectEqualStrings("beta committed", parsed_updated_b.value.title);

    const stale_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{
            .{ .table_name = "docs", .key = "doc:a", .version = left.version.? },
            .{ .table_name = "docs", .key = "doc:b", .version = right.version.? },
        },
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        null,
    );
    defer std.heap.page_allocator.free(stale_body);

    var aborted = try client.fetchTransactionCommit(client_base, stale_body);
    defer aborted.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 409), aborted.status);
    var parsed_abort = try std.json.parseFromSlice(transactions_api.CommitResponse, std.heap.page_allocator, aborted.body, .{});
    defer parsed_abort.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_abort.value.status);
    try std.testing.expect(parsed_abort.value.conflict != null);
    const conflict = parsed_abort.value.conflict.?;
    try std.testing.expectEqualStrings("version_conflict", conflict.kind);
    const participant = conflict.participant.?;
    try std.testing.expectEqualStrings("prepare", participant.phase.?);
    try std.testing.expectEqual(@as(u64, group_id), participant.group_id.?);
}

test "public api multi-node e2e commits cross-table transactions atomically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6144, root_a, cat_a),
        makeHostSimConfig(2, 6144, root_b, cat_b),
        makeHostSimConfig(3, 6144, root_c, cat_c),
        makeHostSimConfig(4, 6144, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6144, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_users_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "users");
    defer std.heap.page_allocator.free(create_users_body);
    _ = try client.createTable(api_base_uris[0], "users", create_users_body);

    const create_orders_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "orders");
    defer std.heap.page_allocator.free(create_orders_body);
    _ = try client.createTable(api_base_uris[0], "orders", create_orders_body);

    var users_group: u64 = 0;
    var orders_group: u64 = 0;
    var client_index: ?usize = null;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_tables = try cluster.node(0).listProjectedTables(std.testing.allocator);
        defer cluster.node(0).freeProjectedTables(std.testing.allocator, projected_tables);
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_tables.len < 2 or projected_ranges.len < 2) continue;

        var users_table_id: ?u64 = null;
        var orders_table_id: ?u64 = null;
        for (projected_tables) |table| {
            if (std.mem.eql(u8, table.name, "users")) users_table_id = table.table_id;
            if (std.mem.eql(u8, table.name, "orders")) orders_table_id = table.table_id;
        }
        if (users_table_id == null or orders_table_id == null) continue;

        for (projected_ranges) |range| {
            if (range.table_id == users_table_id.?) users_group = range.group_id;
            if (range.table_id == orders_table_id.?) orders_group = range.group_id;
        }
        if (users_group == 0 or orders_group == 0) continue;

        for (0..4) |i| {
            if (cluster.node(i).status(users_group) != .active or cluster.node(i).status(orders_group) != .active) {
                client_index = i;
                break;
            }
        }
        if (client_index != null) break;
    }
    const client_base = api_base_uris[client_index orelse 0];
    try std.testing.expect(users_group != 0);
    try std.testing.expect(orders_group != 0);

    const seed_orders_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"order:old":{"user":"user:0","item":"legacy","qty":1}}}
    );
    defer std.heap.page_allocator.free(seed_orders_body);
    var seeded_orders = try client.fetchBatch(client_base, "orders", seed_orders_body);
    defer seeded_orders.deinit(std.heap.page_allocator);

    const users_insert_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"user:1":{"name":"Alice","email":"alice@example.com"},"user:2":{"name":"Bob","email":"bob@example.com"}}}
    );
    defer std.heap.page_allocator.free(users_insert_batch);
    const orders_insert_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"order:1":{"user":"user:1","item":"widget","qty":5},"order:2":{"user":"user:2","item":"gadget","qty":3}}}
    );
    defer std.heap.page_allocator.free(orders_insert_batch);
    const cross_commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{},
        &.{
            .{ .table_name = "users", .batch_json = users_insert_batch },
            .{ .table_name = "orders", .batch_json = orders_insert_batch },
        },
        "write",
    );
    defer std.heap.page_allocator.free(cross_commit_body);

    var committed = try client.fetchTransactionCommit(client_base, cross_commit_body);
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.heap.page_allocator, committed.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);

    var user_one = try client.fetchLookup(client_base, "users", "user:1", null);
    defer user_one.deinit(std.heap.page_allocator);
    var parsed_user_one = try parseJsonBody(UserName, user_one.body);
    defer parsed_user_one.deinit();
    try std.testing.expectEqualStrings("Alice", parsed_user_one.value.name);
    var order_one = try client.fetchLookup(client_base, "orders", "order:1", null);
    defer order_one.deinit(std.heap.page_allocator);
    var parsed_order_one = try parseJsonBody(OrderItem, order_one.body);
    defer parsed_order_one.deinit();
    try std.testing.expectEqualStrings("widget", parsed_order_one.value.item);

    const users_mixed_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"user:3":{"name":"Charlie"}}}
    );
    defer std.heap.page_allocator.free(users_mixed_batch);
    const orders_delete_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"deletes":["order:old"]}
    );
    defer std.heap.page_allocator.free(orders_delete_batch);
    const mixed_commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{},
        &.{
            .{ .table_name = "users", .batch_json = users_mixed_batch },
            .{ .table_name = "orders", .batch_json = orders_delete_batch },
        },
        "write",
    );
    defer std.heap.page_allocator.free(mixed_commit_body);

    var mixed_commit = try client.fetchTransactionCommit(client_base, mixed_commit_body);
    defer mixed_commit.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), mixed_commit.status);

    var user_three = try client.fetchLookup(client_base, "users", "user:3", null);
    defer user_three.deinit(std.heap.page_allocator);
    var parsed_user_three = try parseJsonBody(UserName, user_three.body);
    defer parsed_user_three.deinit();
    try std.testing.expectEqualStrings("Charlie", parsed_user_three.value.name);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "orders", "order:old", null));

    const invalid_users_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"user:phantom":{"name":"Phantom"}}}
    );
    defer std.heap.page_allocator.free(invalid_users_batch);
    const invalid_missing_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"missing:1":{"data":"should fail"}}}
    );
    defer std.heap.page_allocator.free(invalid_missing_batch);
    const invalid_commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{},
        &.{
            .{ .table_name = "users", .batch_json = invalid_users_batch },
            .{ .table_name = "missing", .batch_json = invalid_missing_batch },
        },
        "write",
    );
    defer std.heap.page_allocator.free(invalid_commit_body);

    const commit_uri = try std.fmt.allocPrint(std.heap.page_allocator, "{s}{s}", .{ client_base, api_routes.Routes.transactions_commit });
    defer std.heap.page_allocator.free(commit_uri);
    var invalid_resp = try client_executor.executor().execute(std.heap.page_allocator, .{
        .method = .POST,
        .uri = commit_uri,
        .content_type = "application/json",
        .body = invalid_commit_body,
    });
    defer invalid_resp.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), invalid_resp.status);

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "users", "user:phantom", null));
}

test "public api multi-node e2e supports long-lived transaction sessions from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-txn-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6149, root_a, cat_a),
        makeHostSimConfig(2, 6149, root_b, cat_b),
        makeHostSimConfig(3, 6149, root_c, cat_c),
        makeHostSimConfig(4, 6149, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6149, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node session txn docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var group_id: u64 = 0;
    var non_host_index: ?usize = null;
    var active_count: usize = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        group_id = projected_ranges[0].group_id;
        active_count = 0;
        non_host_index = null;
        for (0..4) |i| {
            if (cluster.node(i).status(group_id) == .active) {
                active_count += 1;
            } else {
                non_host_index = i;
            }
        }
        if (active_count == 3 and non_host_index != null) break;
    }
    try std.testing.expectEqual(@as(usize, 3), active_count);

    const begin_index = non_host_index orelse return error.TestExpectedEqual;
    const followup_index = (begin_index + 1) % 4;
    const begin_base = api_base_uris[begin_index];
    const followup_base = api_base_uris[followup_index];
    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello session txn"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(begin_base, "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var lookup = try client.fetchLookup(begin_base, "docs", "doc:a", null);
    defer lookup.deinit(std.heap.page_allocator);

    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(begin_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_stage_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_stage_body);
    var read_stage = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_stage_body);
    defer read_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_stage.status);
    var parsed_read_stage = try parsePageJson(transactions_api.StageReadResponse, read_stage.body);
    defer parsed_read_stage.deinit();
    try std.testing.expectEqualStrings("7", parsed_read_stage.value.snapshot.version);
    try std.testing.expectEqualStrings("alpha", parsed_read_stage.value.snapshot.document.object.get("title").?.string);

    const external_update_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha external","body":"external session"}}}
    );
    defer std.heap.page_allocator.free(external_update_body);
    var external_update = try client.fetchBatch(begin_base, "docs", external_update_body);
    defer external_update.deinit(std.heap.page_allocator);

    var repeated_read_stage = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_stage_body);
    defer repeated_read_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), repeated_read_stage.status);
    var parsed_repeated_read_stage = try parsePageJson(transactions_api.StageReadResponse, repeated_read_stage.body);
    defer parsed_repeated_read_stage.deinit();
    try std.testing.expectEqualStrings("7", parsed_repeated_read_stage.value.snapshot.version);
    try std.testing.expectEqualStrings("alpha", parsed_repeated_read_stage.value.snapshot.document.object.get("title").?.string);

    const write_stage_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        "{\"title\":\"alpha session committed\",\"body\":\"session committed\"}",
    );
    defer std.heap.page_allocator.free(write_stage_body);
    var write_stage = try client.fetchTransactionSessionWrite(followup_base, txn_id_hex, write_stage_body);
    defer write_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_stage.status);

    var session_info = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer session_info.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), session_info.status);
    var parsed_session_info = try parsePageJson(transactions_api.SessionDetailsResponse, session_info.body);
    defer parsed_session_info.deinit();
    try std.testing.expectEqual(@as(u64, @intCast(begin_index + 1)), parsed_session_info.value.owner_node_id);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.staged_write_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.read_snapshot_count);
    try std.testing.expect(parsed_session_info.value.lease_expires_at > 0);
    try std.testing.expectEqualStrings("held", parsed_session_info.value.lease_state);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.tables.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.read_snapshots.len);
    try std.testing.expectEqualStrings("docs", parsed_session_info.value.read_snapshots[0].table);
    try std.testing.expectEqualStrings("doc:a", parsed_session_info.value.read_snapshots[0].key);
    try std.testing.expectEqualStrings("docs", parsed_session_info.value.tables[0].table);
    try std.testing.expectEqual(@as(usize, 0), parsed_session_info.value.savepoint_ids.len);

    var owner_session_list = try client.fetchTransactionSessions(begin_base);
    defer owner_session_list.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), owner_session_list.status);
    var parsed_owner_session_list = try parsePageJson(transactions_api.SessionListResponse, owner_session_list.body);
    defer parsed_owner_session_list.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_owner_session_list.value.session_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_owner_session_list.value.lease_held_count);
    try std.testing.expectEqual(@as(usize, 0), parsed_owner_session_list.value.lease_expired_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_owner_session_list.value.sessions.len);
    try std.testing.expect(parsed_owner_session_list.value.sessions[0].lease_expires_at > 0);
    try std.testing.expectEqualStrings("held", parsed_owner_session_list.value.sessions[0].lease_state);

    var savepoint = try client.fetchTransactionSessionSavepoint(followup_base, txn_id_hex);
    defer savepoint.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), savepoint.status);
    var parsed_savepoint = try parsePageJson(transactions_api.SavepointStatusResponse, savepoint.body);
    defer parsed_savepoint.deinit();
    const savepoint_id = parsed_savepoint.value.savepoint_id;

    var session_info_with_savepoint = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer session_info_with_savepoint.deinit(std.heap.page_allocator);
    var parsed_session_info_with_savepoint = try parsePageJson(transactions_api.SessionDetailsResponse, session_info_with_savepoint.body);
    defer parsed_session_info_with_savepoint.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_with_savepoint.value.read_snapshots.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_with_savepoint.value.savepoint_ids.len);
    try std.testing.expectEqual(savepoint_id, parsed_session_info_with_savepoint.value.savepoint_ids[0]);

    const delete_stage_committed = try test_contract_helpers.encodeTransactionStageDeleteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
    );
    defer std.heap.page_allocator.free(delete_stage_committed);
    var delete_stage_committed_resp = try client.fetchTransactionSessionDelete(followup_base, txn_id_hex, delete_stage_committed);
    defer delete_stage_committed_resp.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), delete_stage_committed_resp.status);

    var rollback = try client.fetchTransactionSessionRollback(followup_base, txn_id_hex, savepoint_id);
    defer rollback.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), rollback.status);

    var committed = try client.fetchTransactionSessionCommit(followup_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);

    var updated = try client.fetchLookup(followup_base, "docs", "doc:a", null);
    defer updated.deinit(std.heap.page_allocator);
    var parsed_updated = try parseJsonBody(LookupTitle, updated.body);
    defer parsed_updated.deinit();
    try std.testing.expectEqualStrings("alpha session committed", parsed_updated.value.title);

    var latest_lookup = try client.fetchLookup(followup_base, "docs", "doc:a", null);
    defer latest_lookup.deinit(std.heap.page_allocator);

    var begin_stale = try client.fetchTransactionBegin(begin_base, "{}");
    defer begin_stale.deinit(std.heap.page_allocator);
    var parsed_begin_stale = try parsePageJson(transactions_api.BeginResponse, begin_stale.body);
    defer parsed_begin_stale.deinit();
    const stale_txn_id_hex = parsed_begin_stale.value.transaction_id;

    const stale_read_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        latest_lookup.version.?,
    );
    defer std.heap.page_allocator.free(stale_read_body);
    var stale_read = try client.fetchTransactionSessionRead(followup_base, stale_txn_id_hex, stale_read_body);
    defer stale_read.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), stale_read.status);

    const stale_external_update_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha session external","body":"external session conflict"}}}
    );
    defer std.heap.page_allocator.free(stale_external_update_body);
    var stale_external_update = try client.fetchBatch(begin_base, "docs", stale_external_update_body);
    defer stale_external_update.deinit(std.heap.page_allocator);

    const stale_write_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        "{\"title\":\"alpha session stale\",\"body\":\"stale session commit\"}",
    );
    defer std.heap.page_allocator.free(stale_write_body);
    var stale_write = try client.fetchTransactionSessionWrite(followup_base, stale_txn_id_hex, stale_write_body);
    defer stale_write.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), stale_write.status);

    var stale_commit = try client.fetchTransactionSessionCommit(followup_base, stale_txn_id_hex, "");
    defer stale_commit.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 409), stale_commit.status);
    var parsed_stale_commit = try parsePageJson(transactions_api.SessionCommitResponse, stale_commit.body);
    defer parsed_stale_commit.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_stale_commit.value.status);
    const stale_conflict = parsed_stale_commit.value.conflict.?;
    try std.testing.expectEqualStrings("version_conflict", stale_conflict.kind);
    const stale_participant = stale_conflict.participant.?;
    try std.testing.expectEqualStrings("prepare", stale_participant.phase.?);
    try std.testing.expectEqual(@as(u64, group_id), stale_participant.group_id.?);
    try std.testing.expectEqual(try std.fmt.parseInt(u64, latest_lookup.version.?, 10), stale_conflict.expected_version.?);
    try std.testing.expect(stale_conflict.current_version.? > stale_conflict.expected_version.?);

    var begin_abort = try client.fetchTransactionBegin(begin_base, "{}");
    defer begin_abort.deinit(std.heap.page_allocator);
    var parsed_begin_abort = try parsePageJson(transactions_api.BeginResponse, begin_abort.body);
    defer parsed_begin_abort.deinit();
    const abort_txn_id_hex = parsed_begin_abort.value.transaction_id;

    const delete_stage_body = try test_contract_helpers.encodeTransactionStageDeleteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
    );
    defer std.heap.page_allocator.free(delete_stage_body);
    var delete_stage = try client.fetchTransactionSessionDelete(followup_base, abort_txn_id_hex, delete_stage_body);
    defer delete_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), delete_stage.status);

    var aborted = try client.fetchTransactionAbort(followup_base, abort_txn_id_hex);
    defer aborted.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), aborted.status);

    var commit_after_abort = try client.fetchTransactionSessionCommit(followup_base, abort_txn_id_hex, "");
    defer commit_after_abort.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), commit_after_abort.status);
}

test "public api multi-node e2e supports cross-table transaction sessions" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6154, root_a, cat_a),
        makeHostSimConfig(2, 6154, root_b, cat_b),
        makeHostSimConfig(3, 6154, root_c, cat_c),
        makeHostSimConfig(4, 6154, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6154, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_users_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "users");
    defer std.heap.page_allocator.free(create_users_body);
    _ = try client.createTable(api_base_uris[0], "users", create_users_body);

    const create_orders_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "orders");
    defer std.heap.page_allocator.free(create_orders_body);
    _ = try client.createTable(api_base_uris[0], "orders", create_orders_body);

    var users_group: u64 = 0;
    var orders_group: u64 = 0;
    var non_host_index: ?usize = null;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_tables = try cluster.node(0).listProjectedTables(std.testing.allocator);
        defer cluster.node(0).freeProjectedTables(std.testing.allocator, projected_tables);
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_tables.len < 2 or projected_ranges.len < 2) continue;

        var users_table_id: ?u64 = null;
        var orders_table_id: ?u64 = null;
        for (projected_tables) |table| {
            if (std.mem.eql(u8, table.name, "users")) users_table_id = table.table_id;
            if (std.mem.eql(u8, table.name, "orders")) orders_table_id = table.table_id;
        }
        if (users_table_id == null or orders_table_id == null) continue;

        for (projected_ranges) |range| {
            if (range.table_id == users_table_id.?) users_group = range.group_id;
            if (range.table_id == orders_table_id.?) orders_group = range.group_id;
        }
        if (users_group == 0 or orders_group == 0) continue;

        for (0..4) |i| {
            if (cluster.node(i).status(users_group) != .active or cluster.node(i).status(orders_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    const begin_index = non_host_index orelse 0;
    const followup_index = (begin_index + 1) % 4;
    const begin_base = api_base_uris[begin_index];
    const followup_base = api_base_uris[followup_index];
    try std.testing.expect(users_group != 0);
    try std.testing.expect(orders_group != 0);

    const seed_users_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"user:1":{"name":"Alice","email":"alice@example.com"}}}
    );
    defer std.heap.page_allocator.free(seed_users_body);
    var seed_users = try client.fetchBatch(begin_base, "users", seed_users_body);
    defer seed_users.deinit(std.heap.page_allocator);

    const seed_orders_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"order:old":{"user":"user:1","item":"legacy","qty":1}}}
    );
    defer std.heap.page_allocator.free(seed_orders_body);
    var seed_orders = try client.fetchBatch(begin_base, "orders", seed_orders_body);
    defer seed_orders.deinit(std.heap.page_allocator);

    var user_lookup = try client.fetchLookup(begin_base, "users", "user:1", null);
    defer user_lookup.deinit(std.heap.page_allocator);
    var order_lookup = try client.fetchLookup(begin_base, "orders", "order:old", null);
    defer order_lookup.deinit(std.heap.page_allocator);

    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(begin_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_user_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "users",
        "user:1",
        user_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_user_body);
    var read_user = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_user_body);
    defer read_user.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_user.status);

    const read_order_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "orders",
        "order:old",
        order_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_order_body);
    var read_order = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_order_body);
    defer read_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_order.status);

    const write_user_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "users",
        "user:1",
        "{\"name\":\"Alice Session\",\"email\":\"alice-session@example.com\"}",
    );
    defer std.heap.page_allocator.free(write_user_body);
    var write_user = try client.fetchTransactionSessionWrite(followup_base, txn_id_hex, write_user_body);
    defer write_user.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_user.status);

    const delete_order_body = try test_contract_helpers.encodeTransactionStageDeleteRequest(
        std.heap.page_allocator,
        "orders",
        "order:old",
    );
    defer std.heap.page_allocator.free(delete_order_body);
    var delete_order = try client.fetchTransactionSessionDelete(followup_base, txn_id_hex, delete_order_body);
    defer delete_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), delete_order.status);

    var session_info = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer session_info.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), session_info.status);
    var parsed_session_info = try parsePageJson(transactions_api.SessionDetailsResponse, session_info.body);
    defer parsed_session_info.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_session_info.value.staged_table_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.staged_write_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.staged_delete_count);
    try std.testing.expectEqual(@as(usize, 2), parsed_session_info.value.read_snapshot_count);

    var committed = try client.fetchTransactionSessionCommit(followup_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);

    var updated_user = try client.fetchLookup(followup_base, "users", "user:1", null);
    defer updated_user.deinit(std.heap.page_allocator);
    var parsed_updated_user = try parseJsonBody(UserName, updated_user.body);
    defer parsed_updated_user.deinit();
    try std.testing.expectEqualStrings("Alice Session", parsed_updated_user.value.name);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(followup_base, "orders", "order:old", null));
}

test "public api multi-node e2e reloads durable cross-table transaction sessions after coordinator restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6155, root_a, cat_a),
        makeHostSimConfig(2, 6155, root_b, cat_b),
        makeHostSimConfig(3, 6155, root_c, cat_c),
        makeHostSimConfig(4, 6155, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6155, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    const session_path_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-a-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_a);
    const session_path_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-b-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_b);
    const session_path_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-c-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_c);
    const session_path_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-restart-d-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_d);
    const session_paths = [_][]const u8{ session_path_a, session_path_b, session_path_c, session_path_d };

    var session_store_paths: [4][:0]u8 = undefined;
    defer for (session_store_paths) |path_z| std.testing.allocator.free(path_z);
    var session_docstores: [4]docstore_mod.DocStore = undefined;
    defer for (&session_docstores) |*store| store.close();
    var durable_session_stores: [4]transactions_api.DurableSessionStore = undefined;
    var session_store_ptrs: [4]?*transactions_api.DurableSessionStore = undefined;
    for (session_paths, 0..) |path, i| {
        session_store_paths[i] = try std.testing.allocator.dupeZ(u8, path);
        session_docstores[i] = try docstore_mod.DocStore.open(std.testing.allocator, session_store_paths[i], .{});
        durable_session_stores[i] = transactions_api.DurableSessionStore.init(std.testing.allocator, &session_docstores[i]);
        session_store_ptrs[i] = &durable_session_stores[i];
    }

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServersWithDurableSessions(
        4,
        &cluster,
        &roots,
        forward_executor.executor(),
        &session_store_ptrs,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_users_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "users");
    defer std.heap.page_allocator.free(create_users_body);
    _ = try client.createTable(api_base_uris[0], "users", create_users_body);

    const create_orders_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "orders");
    defer std.heap.page_allocator.free(create_orders_body);
    _ = try client.createTable(api_base_uris[0], "orders", create_orders_body);

    var users_group: u64 = 0;
    var orders_group: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_tables = try cluster.node(0).listProjectedTables(std.testing.allocator);
        defer cluster.node(0).freeProjectedTables(std.testing.allocator, projected_tables);
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_tables.len < 2 or projected_ranges.len < 2) continue;

        var users_table_id: ?u64 = null;
        var orders_table_id: ?u64 = null;
        for (projected_tables) |table| {
            if (std.mem.eql(u8, table.name, "users")) users_table_id = table.table_id;
            if (std.mem.eql(u8, table.name, "orders")) orders_table_id = table.table_id;
        }
        if (users_table_id == null or orders_table_id == null) continue;

        for (projected_ranges) |range| {
            if (range.table_id == users_table_id.?) users_group = range.group_id;
            if (range.table_id == orders_table_id.?) orders_group = range.group_id;
        }
        if (users_group != 0 and orders_group != 0) break;
    }
    try std.testing.expect(users_group != 0);
    try std.testing.expect(orders_group != 0);

    const begin_index: usize = 0;
    const followup_index: usize = 1;
    const begin_base = api_base_uris[begin_index];
    const followup_base = api_base_uris[followup_index];

    const seed_users_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"user:1":{"name":"Alice","email":"alice@example.com"}}}
    );
    defer std.heap.page_allocator.free(seed_users_body);
    var seed_users = try client.fetchBatch(begin_base, "users", seed_users_body);
    defer seed_users.deinit(std.heap.page_allocator);

    const seed_orders_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"order:old":{"user":"user:1","item":"legacy","qty":1}}}
    );
    defer std.heap.page_allocator.free(seed_orders_body);
    var seed_orders = try client.fetchBatch(begin_base, "orders", seed_orders_body);
    defer seed_orders.deinit(std.heap.page_allocator);

    var user_lookup = try client.fetchLookup(begin_base, "users", "user:1", null);
    defer user_lookup.deinit(std.heap.page_allocator);
    var order_lookup = try client.fetchLookup(begin_base, "orders", "order:old", null);
    defer order_lookup.deinit(std.heap.page_allocator);

    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(begin_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_user_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "users",
        "user:1",
        user_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_user_body);
    var read_user = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_user_body);
    defer read_user.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_user.status);

    const read_order_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "orders",
        "order:old",
        order_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_order_body);
    var read_order = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_order_body);
    defer read_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_order.status);

    const write_user_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "users",
        "user:1",
        "{\"name\":\"Alice Durable Session\",\"email\":\"alice-durable@example.com\"}",
    );
    defer std.heap.page_allocator.free(write_user_body);
    var write_user = try client.fetchTransactionSessionWrite(followup_base, txn_id_hex, write_user_body);
    defer write_user.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_user.status);

    const delete_order_body = try test_contract_helpers.encodeTransactionStageDeleteRequest(
        std.heap.page_allocator,
        "orders",
        "order:old",
    );
    defer std.heap.page_allocator.free(delete_order_body);
    var delete_order = try client.fetchTransactionSessionDelete(followup_base, txn_id_hex, delete_order_body);
    defer delete_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), delete_order.status);

    listeners[begin_index].deinit();
    std.testing.allocator.free(api_base_uris[begin_index]);
    servers[begin_index] = api_http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{
            .session_router = routers[begin_index].iface(),
            .session_executor = forward_executor.executor(),
            .session_store = &durable_session_stores[begin_index],
        },
        status_sources[begin_index].iface(),
        read_sources[begin_index].source(),
        write_sources[begin_index].source(),
    );
    listeners[begin_index] = std_http_listener.StdHttpListener.init(
        std.testing.allocator,
        .{},
        servers[begin_index].executor(),
    );
    try listeners[begin_index].start();
    api_base_uris[begin_index] = try listeners[begin_index].baseUri(std.testing.allocator);

    var session_info_after_restart = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer session_info_after_restart.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), session_info_after_restart.status);
    var parsed_session_info_after_restart = try parsePageJson(transactions_api.SessionDetailsResponse, session_info_after_restart.body);
    defer parsed_session_info_after_restart.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_session_info_after_restart.value.staged_table_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_after_restart.value.staged_write_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_after_restart.value.staged_delete_count);
    try std.testing.expectEqual(@as(usize, 2), parsed_session_info_after_restart.value.read_snapshot_count);

    var committed = try client.fetchTransactionSessionCommit(followup_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);

    var updated_user = try client.fetchLookup(followup_base, "users", "user:1", null);
    defer updated_user.deinit(std.heap.page_allocator);
    var parsed_updated_user = try parseJsonBody(UserName, updated_user.body);
    defer parsed_updated_user.deinit();
    try std.testing.expectEqualStrings("Alice Durable Session", parsed_updated_user.value.name);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(followup_base, "orders", "order:old", null));
}

test "public api multi-node e2e adopts durable cross-table transaction sessions after coordinator loss" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6156, root_a, cat_a),
        makeHostSimConfig(2, 6156, root_b, cat_b),
        makeHostSimConfig(3, 6156, root_c, cat_c),
        makeHostSimConfig(4, 6156, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6156, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    const shared_session_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-adopt-shared", .{tmp.sub_path});
    defer std.testing.allocator.free(shared_session_path);

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServersWithSharedSessionStorePath(
        4,
        &cluster,
        &roots,
        &forward_executor,
        shared_session_path,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&servers) |*server| server.deinit();
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_users_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "users");
    defer std.heap.page_allocator.free(create_users_body);
    _ = try client.createTable(api_base_uris[0], "users", create_users_body);

    const create_orders_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "orders");
    defer std.heap.page_allocator.free(create_orders_body);
    _ = try client.createTable(api_base_uris[0], "orders", create_orders_body);

    var users_group: u64 = 0;
    var orders_group: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_tables = try cluster.node(0).listProjectedTables(std.testing.allocator);
        defer cluster.node(0).freeProjectedTables(std.testing.allocator, projected_tables);
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_tables.len < 2 or projected_ranges.len < 2) continue;

        var users_table_id: ?u64 = null;
        var orders_table_id: ?u64 = null;
        for (projected_tables) |table| {
            if (std.mem.eql(u8, table.name, "users")) users_table_id = table.table_id;
            if (std.mem.eql(u8, table.name, "orders")) orders_table_id = table.table_id;
        }
        if (users_table_id == null or orders_table_id == null) continue;

        for (projected_ranges) |range| {
            if (range.table_id == users_table_id.?) users_group = range.group_id;
            if (range.table_id == orders_table_id.?) orders_group = range.group_id;
        }
        if (users_group != 0 and orders_group != 0) break;
    }
    try std.testing.expect(users_group != 0);
    try std.testing.expect(orders_group != 0);

    const begin_index: usize = 0;
    const followup_index: usize = 1;
    const begin_base = api_base_uris[begin_index];
    const followup_base = api_base_uris[followup_index];

    const seed_users_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"user:1":{"name":"Alice","email":"alice@example.com"}}}
    );
    defer std.heap.page_allocator.free(seed_users_body);
    var seed_users = try client.fetchBatch(begin_base, "users", seed_users_body);
    defer seed_users.deinit(std.heap.page_allocator);

    const seed_orders_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"order:old":{"user":"user:1","item":"legacy","qty":1}}}
    );
    defer std.heap.page_allocator.free(seed_orders_body);
    var seed_orders = try client.fetchBatch(begin_base, "orders", seed_orders_body);
    defer seed_orders.deinit(std.heap.page_allocator);

    var user_lookup = try client.fetchLookup(begin_base, "users", "user:1", null);
    defer user_lookup.deinit(std.heap.page_allocator);
    var order_lookup = try client.fetchLookup(begin_base, "orders", "order:old", null);
    defer order_lookup.deinit(std.heap.page_allocator);

    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(begin_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_user_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "users",
        "user:1",
        user_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_user_body);
    var read_user = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_user_body);
    defer read_user.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_user.status);

    const read_order_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "orders",
        "order:old",
        order_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_order_body);
    var read_order = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_order_body);
    defer read_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_order.status);

    const write_user_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "users",
        "user:1",
        "{\"name\":\"Alice Adopted Session\",\"email\":\"alice-adopted@example.com\"}",
    );
    defer std.heap.page_allocator.free(write_user_body);
    var write_user = try client.fetchTransactionSessionWrite(followup_base, txn_id_hex, write_user_body);
    defer write_user.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_user.status);

    const delete_order_body = try test_contract_helpers.encodeTransactionStageDeleteRequest(
        std.heap.page_allocator,
        "orders",
        "order:old",
    );
    defer std.heap.page_allocator.free(delete_order_body);
    var delete_order = try client.fetchTransactionSessionDelete(followup_base, txn_id_hex, delete_order_body);
    defer delete_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), delete_order.status);

    listeners[begin_index].deinit();
    std.testing.allocator.free(api_base_uris[begin_index]);
    api_base_uris[begin_index] = try std.testing.allocator.dupe(u8, "http://127.0.0.1:1");

    var adopted_info = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer adopted_info.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), adopted_info.status);
    var parsed_adopted_info = try parsePageJson(transactions_api.SessionDetailsResponse, adopted_info.body);
    defer parsed_adopted_info.deinit();
    try std.testing.expectEqual(@as(u64, @intCast(followup_index + 1)), parsed_adopted_info.value.owner_node_id);
    try std.testing.expectEqual(@as(usize, 2), parsed_adopted_info.value.staged_table_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_adopted_info.value.staged_write_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_adopted_info.value.staged_delete_count);
    try std.testing.expect(parsed_adopted_info.value.lease_expires_at > 0);
    try std.testing.expectEqualStrings("held", parsed_adopted_info.value.lease_state);

    var committed = try client.fetchTransactionSessionCommit(followup_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);

    var session_info = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer session_info.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), session_info.status);

    var updated_user = try client.fetchLookup(followup_base, "users", "user:1", null);
    defer updated_user.deinit(std.heap.page_allocator);
    var parsed_updated_user = try parseJsonBody(UserName, updated_user.body);
    defer parsed_updated_user.deinit();
    try std.testing.expectEqualStrings("Alice Adopted Session", parsed_updated_user.value.name);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(followup_base, "orders", "order:old", null));
}

test "public api multi-node e2e reloads durable transaction sessions after coordinator restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6151, root_a, cat_a),
        makeHostSimConfig(2, 6151, root_b, cat_b),
        makeHostSimConfig(3, 6151, root_c, cat_c),
        makeHostSimConfig(4, 6151, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6151, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    const session_path_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-a-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_a);
    const session_path_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-b-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_b);
    const session_path_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-c-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_c);
    const session_path_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-restart-d-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path_d);
    const session_paths = [_][]const u8{ session_path_a, session_path_b, session_path_c, session_path_d };

    var session_store_paths: [4][:0]u8 = undefined;
    defer for (session_store_paths) |path_z| std.testing.allocator.free(path_z);
    var session_docstores: [4]docstore_mod.DocStore = undefined;
    defer for (&session_docstores) |*store| store.close();
    var durable_session_stores: [4]transactions_api.DurableSessionStore = undefined;
    var session_store_ptrs: [4]?*transactions_api.DurableSessionStore = undefined;
    for (session_paths, 0..) |path, i| {
        session_store_paths[i] = try std.testing.allocator.dupeZ(u8, path);
        session_docstores[i] = try docstore_mod.DocStore.open(std.testing.allocator, session_store_paths[i], .{});
        durable_session_stores[i] = transactions_api.DurableSessionStore.init(std.testing.allocator, &session_docstores[i]);
        session_store_ptrs[i] = &durable_session_stores[i];
    }

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServersWithDurableSessions(
        4,
        &cluster,
        &roots,
        forward_executor.executor(),
        &session_store_ptrs,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node durable session txn docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var group_id: u64 = 0;
    var non_host_index: ?usize = null;
    var active_count: usize = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        group_id = projected_ranges[0].group_id;
        active_count = 0;
        non_host_index = null;
        for (0..4) |i| {
            if (cluster.node(i).status(group_id) == .active) {
                active_count += 1;
            } else {
                non_host_index = i;
            }
        }
        if (active_count == 3 and non_host_index != null) break;
    }
    try std.testing.expectEqual(@as(usize, 3), active_count);

    const begin_index = non_host_index orelse return error.TestExpectedEqual;
    const followup_index = (begin_index + 1) % 4;
    const begin_base = api_base_uris[begin_index];
    const followup_base = api_base_uris[followup_index];

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello durable session txn"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(begin_base, "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var lookup = try client.fetchLookup(begin_base, "docs", "doc:a", null);
    defer lookup.deinit(std.heap.page_allocator);

    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(begin_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_stage_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_stage_body);
    var read_stage = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_stage_body);
    defer read_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_stage.status);

    const write_stage_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        "{\"title\":\"alpha durable session committed\",\"body\":\"durable session committed\"}",
    );
    defer std.heap.page_allocator.free(write_stage_body);
    var write_stage = try client.fetchTransactionSessionWrite(followup_base, txn_id_hex, write_stage_body);
    defer write_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_stage.status);

    listeners[begin_index].deinit();
    std.testing.allocator.free(api_base_uris[begin_index]);
    servers[begin_index] = api_http_server.ApiHttpServer.init(
        std.testing.allocator,
        .{
            .session_router = routers[begin_index].iface(),
            .session_executor = forward_executor.executor(),
            .session_store = &durable_session_stores[begin_index],
        },
        status_sources[begin_index].iface(),
        read_sources[begin_index].source(),
        write_sources[begin_index].source(),
    );
    listeners[begin_index] = std_http_listener.StdHttpListener.init(
        std.testing.allocator,
        .{},
        servers[begin_index].executor(),
    );
    try listeners[begin_index].start();
    api_base_uris[begin_index] = try listeners[begin_index].baseUri(std.testing.allocator);

    var session_info_after_restart = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer session_info_after_restart.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), session_info_after_restart.status);
    var parsed_session_info_after_restart = try parsePageJson(transactions_api.SessionDetailsResponse, session_info_after_restart.body);
    defer parsed_session_info_after_restart.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_after_restart.value.staged_table_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_after_restart.value.staged_write_count);

    var committed = try client.fetchTransactionSessionCommit(followup_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);

    var updated = try client.fetchLookup(followup_base, "docs", "doc:a", null);
    defer updated.deinit(std.heap.page_allocator);
    var parsed_updated = try parseJsonBody(LookupTitle, updated.body);
    defer parsed_updated.deinit();
    try std.testing.expectEqualStrings("alpha durable session committed", parsed_updated.value.title);
}

test "public api multi-node e2e adopts durable transaction sessions after coordinator loss" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6151, root_a, cat_a),
        makeHostSimConfig(2, 6151, root_b, cat_b),
        makeHostSimConfig(3, 6151, root_c, cat_c),
        makeHostSimConfig(4, 6151, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6151, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    const shared_session_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-adopt-shared", .{tmp.sub_path});
    defer std.testing.allocator.free(shared_session_path);

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServersWithSharedSessionStorePath(
        4,
        &cluster,
        &roots,
        &forward_executor,
        shared_session_path,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&servers) |*server| server.deinit();
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node durable session adopt docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var group_id: u64 = 0;
    var non_host_index: ?usize = null;
    var active_count: usize = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        group_id = projected_ranges[0].group_id;
        active_count = 0;
        non_host_index = null;
        for (0..4) |i| {
            if (cluster.node(i).status(group_id) == .active) {
                active_count += 1;
            } else {
                non_host_index = i;
            }
        }
        if (active_count == 3 and non_host_index != null) break;
    }
    try std.testing.expectEqual(@as(usize, 3), active_count);

    const begin_index = non_host_index orelse return error.TestExpectedEqual;
    const followup_index = (begin_index + 1) % 4;
    const begin_base = api_base_uris[begin_index];
    const followup_base = api_base_uris[followup_index];

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello durable session adopt"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(begin_base, "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var lookup = try client.fetchLookup(begin_base, "docs", "doc:a", null);
    defer lookup.deinit(std.heap.page_allocator);

    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(begin_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_stage_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_stage_body);
    var read_stage = try client.fetchTransactionSessionRead(followup_base, txn_id_hex, read_stage_body);
    defer read_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_stage.status);

    const write_stage_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        "{\"title\":\"alpha adopted\",\"body\":\"adopted session committed\"}",
    );
    defer std.heap.page_allocator.free(write_stage_body);
    var write_stage = try client.fetchTransactionSessionWrite(followup_base, txn_id_hex, write_stage_body);
    defer write_stage.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_stage.status);

    listeners[begin_index].deinit();
    std.testing.allocator.free(api_base_uris[begin_index]);
    api_base_uris[begin_index] = try std.testing.allocator.dupe(u8, "http://127.0.0.1:1");

    var adopted_info = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer adopted_info.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), adopted_info.status);
    var parsed_adopted_info = try parsePageJson(transactions_api.SessionDetailsResponse, adopted_info.body);
    defer parsed_adopted_info.deinit();
    try std.testing.expectEqual(@as(u64, @intCast(followup_index + 1)), parsed_adopted_info.value.owner_node_id);
    try std.testing.expect(parsed_adopted_info.value.lease_expires_at > 0);
    try std.testing.expectEqualStrings("held", parsed_adopted_info.value.lease_state);

    var committed = try client.fetchTransactionSessionCommit(followup_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);

    var session_info = try client.fetchTransactionSessionInfo(followup_base, txn_id_hex);
    defer session_info.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), session_info.status);

    var updated = try client.fetchLookup(followup_base, "docs", "doc:a", null);
    defer updated.deinit(std.heap.page_allocator);
    var parsed_updated = try parseJsonBody(LookupTitle, updated.body);
    defer parsed_updated.deinit();
    try std.testing.expectEqualStrings("alpha adopted", parsed_updated.value.title);
}

test "public api multi-node e2e retries transaction commit once after topology churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6146, root_a, cat_a),
        makeHostSimConfig(2, 6146, root_b, cat_b),
        makeHostSimConfig(3, 6146, root_c, cat_c),
        makeHostSimConfig(4, 6146, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6146, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var bootstrap_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var bootstrap_servers: [4]api_http_server.ApiHttpServer = undefined;
    var bootstrap_status_sources: [4]PublicApiStatusSource = undefined;
    var bootstrap_catalog_sources: [4]PublicApiCatalogSource = undefined;
    var bootstrap_routers: [4]PublicApiRouter(4) = undefined;
    var bootstrap_read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var bootstrap_write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var bootstrap_api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var bootstrap_forward_executor: std_http_executor.StdHttpExecutor = undefined;
    bootstrap_forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer bootstrap_forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &bootstrap_forward_executor,
        &bootstrap_listeners,
        &bootstrap_servers,
        &bootstrap_status_sources,
        &bootstrap_catalog_sources,
        &bootstrap_routers,
        &bootstrap_read_sources,
        &bootstrap_write_sources,
        &bootstrap_api_base_uris,
    );
    defer for (&bootstrap_listeners) |*listener| listener.deinit();
    defer for (bootstrap_api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node txn retry docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(bootstrap_api_base_uris[0], "docs", create_body);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (source_group_id != 0) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":614601,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(614601)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(bootstrap_api_base_uris[0], "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var left_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:a", null);
    defer left_lookup.deinit(std.heap.page_allocator);
    var right_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:z", null);
    defer right_lookup.deinit(std.heap.page_allocator);

    var churn_executor = TxnTopologyChurnExecutor{
        .forward = client_executor.executor(),
        .cluster = &cluster,
        .metadata_apis = &metadata_apis,
        .mode = .merge_once,
    };
    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    try startPublicApiServersWithExecutor(
        4,
        &cluster,
        &roots,
        churn_executor.executor(),
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const commit_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha retried","body":"left retried"},"doc:z":{"title":"zeta retried","body":"right retried"}}}
    );
    defer std.heap.page_allocator.free(commit_batch);
    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{
            .{ .table_name = "docs", .key = "doc:a", .version = left_lookup.version.? },
            .{ .table_name = "docs", .key = "doc:z", .version = right_lookup.version.? },
        },
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        null,
    );
    defer std.heap.page_allocator.free(commit_body);

    var committed = try client.fetchTransactionCommit(client_base, commit_body);
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.heap.page_allocator, committed.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);
    try std.testing.expectEqual(@as(u32, 1), churn_executor.trigger_count);

    var updated_left = try client.fetchLookup(client_base, "docs", "doc:a", null);
    defer updated_left.deinit(std.heap.page_allocator);
    var parsed_updated_left = try parseJsonBody(LookupTitle, updated_left.body);
    defer parsed_updated_left.deinit();
    try std.testing.expectEqualStrings("alpha retried", parsed_updated_left.value.title);
    var updated_right = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer updated_right.deinit(std.heap.page_allocator);
    var parsed_updated_right = try parseJsonBody(LookupTitle, updated_right.body);
    defer parsed_updated_right.deinit();
    try std.testing.expectEqualStrings("zeta retried", parsed_updated_right.value.title);
}

test "public api multi-node e2e fails transaction commit after repeated topology churn beyond retry limit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-retry-fail-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6147, root_a, cat_a),
        makeHostSimConfig(2, 6147, root_b, cat_b),
        makeHostSimConfig(3, 6147, root_c, cat_c),
        makeHostSimConfig(4, 6147, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6147, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var bootstrap_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var bootstrap_servers: [4]api_http_server.ApiHttpServer = undefined;
    var bootstrap_status_sources: [4]PublicApiStatusSource = undefined;
    var bootstrap_catalog_sources: [4]PublicApiCatalogSource = undefined;
    var bootstrap_routers: [4]PublicApiRouter(4) = undefined;
    var bootstrap_read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var bootstrap_write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var bootstrap_api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var bootstrap_forward_executor: std_http_executor.StdHttpExecutor = undefined;
    bootstrap_forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer bootstrap_forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &bootstrap_forward_executor,
        &bootstrap_listeners,
        &bootstrap_servers,
        &bootstrap_status_sources,
        &bootstrap_catalog_sources,
        &bootstrap_routers,
        &bootstrap_read_sources,
        &bootstrap_write_sources,
        &bootstrap_api_base_uris,
    );
    defer for (&bootstrap_listeners) |*listener| listener.deinit();
    defer for (bootstrap_api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node txn retry fail docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(bootstrap_api_base_uris[0], "docs", create_body);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (source_group_id != 0) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":614701,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(614701)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(bootstrap_api_base_uris[0], "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var left_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:a", null);
    defer left_lookup.deinit(std.heap.page_allocator);
    var right_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:z", null);
    defer right_lookup.deinit(std.heap.page_allocator);

    var churn_executor = TxnTopologyChurnExecutor{
        .forward = client_executor.executor(),
        .cluster = &cluster,
        .metadata_apis = &metadata_apis,
        .mode = .merge_then_split,
    };
    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    try startPublicApiServersWithExecutor(
        4,
        &cluster,
        &roots,
        churn_executor.executor(),
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const commit_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha retry fail","body":"left retry fail"},"doc:z":{"title":"zeta retry fail","body":"right retry fail"}}}
    );
    defer std.heap.page_allocator.free(commit_batch);
    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{
            .{ .table_name = "docs", .key = "doc:a", .version = left_lookup.version.? },
            .{ .table_name = "docs", .key = "doc:z", .version = right_lookup.version.? },
        },
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        null,
    );
    defer std.heap.page_allocator.free(commit_body);

    var aborted = try client.fetchTransactionCommit(client_base, commit_body);
    defer aborted.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 409), aborted.status);
    var parsed_abort = try std.json.parseFromSlice(transactions_api.CommitResponse, std.heap.page_allocator, aborted.body, .{});
    defer parsed_abort.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_abort.value.status);
    try std.testing.expect(parsed_abort.value.conflict != null);
    try std.testing.expectEqualStrings("topology changed", parsed_abort.value.conflict.?.message);
    try std.testing.expectEqual(@as(u32, 2), churn_executor.trigger_count);
}

test "public api multi-node e2e retries transaction session commit once after topology churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6150, root_a, cat_a),
        makeHostSimConfig(2, 6150, root_b, cat_b),
        makeHostSimConfig(3, 6150, root_c, cat_c),
        makeHostSimConfig(4, 6150, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6150, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var bootstrap_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var bootstrap_servers: [4]api_http_server.ApiHttpServer = undefined;
    var bootstrap_status_sources: [4]PublicApiStatusSource = undefined;
    var bootstrap_catalog_sources: [4]PublicApiCatalogSource = undefined;
    var bootstrap_routers: [4]PublicApiRouter(4) = undefined;
    var bootstrap_read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var bootstrap_write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var bootstrap_api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var bootstrap_forward_executor: std_http_executor.StdHttpExecutor = undefined;
    bootstrap_forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer bootstrap_forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &bootstrap_forward_executor,
        &bootstrap_listeners,
        &bootstrap_servers,
        &bootstrap_status_sources,
        &bootstrap_catalog_sources,
        &bootstrap_routers,
        &bootstrap_read_sources,
        &bootstrap_write_sources,
        &bootstrap_api_base_uris,
    );
    defer for (&bootstrap_listeners) |*listener| listener.deinit();
    defer for (bootstrap_api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node session retry docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(bootstrap_api_base_uris[0], "docs", create_body);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (source_group_id != 0) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":615001,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(615001)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(bootstrap_api_base_uris[0], "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var left_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:a", null);
    defer left_lookup.deinit(std.heap.page_allocator);
    var right_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:z", null);
    defer right_lookup.deinit(std.heap.page_allocator);

    var churn_executor = TxnTopologyChurnExecutor{
        .forward = client_executor.executor(),
        .cluster = &cluster,
        .metadata_apis = &metadata_apis,
        .mode = .merge_once,
    };
    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    try startPublicApiServersWithExecutor(
        4,
        &cluster,
        &roots,
        churn_executor.executor(),
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(client_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_left_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        left_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_left_body);
    var read_left = try client.fetchTransactionSessionRead(client_base, txn_id_hex, read_left_body);
    defer read_left.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_left.status);

    const read_right_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:z",
        right_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_right_body);
    var read_right = try client.fetchTransactionSessionRead(client_base, txn_id_hex, read_right_body);
    defer read_right.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_right.status);

    const write_left_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        "{\"title\":\"alpha session retried\",\"body\":\"left session retried\"}",
    );
    defer std.heap.page_allocator.free(write_left_body);
    var write_left = try client.fetchTransactionSessionWrite(client_base, txn_id_hex, write_left_body);
    defer write_left.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_left.status);

    const write_right_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:z",
        "{\"title\":\"zeta session retried\",\"body\":\"right session retried\"}",
    );
    defer std.heap.page_allocator.free(write_right_body);
    var write_right = try client.fetchTransactionSessionWrite(client_base, txn_id_hex, write_right_body);
    defer write_right.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_right.status);

    var committed = try client.fetchTransactionSessionCommit(client_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try parsePageJson(transactions_api.SessionCommitResponse, committed.body);
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);
    try std.testing.expectEqual(@as(u32, 1), churn_executor.trigger_count);

    var session_info_after_commit = try client.fetchTransactionSessionInfo(client_base, txn_id_hex);
    defer session_info_after_commit.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), session_info_after_commit.status);

    var updated_left = try client.fetchLookup(client_base, "docs", "doc:a", null);
    defer updated_left.deinit(std.heap.page_allocator);
    var parsed_updated_left = try parseJsonBody(LookupTitle, updated_left.body);
    defer parsed_updated_left.deinit();
    try std.testing.expectEqualStrings("alpha session retried", parsed_updated_left.value.title);
    var updated_right = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer updated_right.deinit(std.heap.page_allocator);
    var parsed_updated_right = try parseJsonBody(LookupTitle, updated_right.body);
    defer parsed_updated_right.deinit();
    try std.testing.expectEqualStrings("zeta session retried", parsed_updated_right.value.title);
}

test "public api multi-node e2e retries cross-table transaction session commit once after topology churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-cross-session-retry-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6157, root_a, cat_a),
        makeHostSimConfig(2, 6157, root_b, cat_b),
        makeHostSimConfig(3, 6157, root_c, cat_c),
        makeHostSimConfig(4, 6157, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6157, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var bootstrap_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var bootstrap_servers: [4]api_http_server.ApiHttpServer = undefined;
    var bootstrap_status_sources: [4]PublicApiStatusSource = undefined;
    var bootstrap_catalog_sources: [4]PublicApiCatalogSource = undefined;
    var bootstrap_routers: [4]PublicApiRouter(4) = undefined;
    var bootstrap_read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var bootstrap_write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var bootstrap_api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var bootstrap_forward_executor: std_http_executor.StdHttpExecutor = undefined;
    bootstrap_forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer bootstrap_forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &bootstrap_forward_executor,
        &bootstrap_listeners,
        &bootstrap_servers,
        &bootstrap_status_sources,
        &bootstrap_catalog_sources,
        &bootstrap_routers,
        &bootstrap_read_sources,
        &bootstrap_write_sources,
        &bootstrap_api_base_uris,
    );
    defer for (&bootstrap_listeners) |*listener| listener.deinit();
    defer for (bootstrap_api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_docs_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node cross session retry docs");
    defer std.heap.page_allocator.free(create_docs_body);
    _ = try client.createTable(bootstrap_api_base_uris[0], "docs", create_docs_body);

    const create_orders_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node cross session retry orders");
    defer std.heap.page_allocator.free(create_orders_body);
    _ = try client.createTable(bootstrap_api_base_uris[0], "orders", create_orders_body);

    var snapshot = try cluster.node(0).adminSnapshot();
    defer cluster.node(0).freeAdminSnapshot(&snapshot);
    const docs_table = findAdminTableByName(&snapshot, "docs") orelse return error.TableNotFound;

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        for (projected_ranges) |range| {
            if (range.table_id == docs_table.table_id) {
                source_group_id = range.group_id;
                break;
            }
        }
        if (source_group_id != 0) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":615701,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(615701)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const docs_batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer std.heap.page_allocator.free(docs_batch_body);
    var docs_batch = try client.fetchBatch(bootstrap_api_base_uris[0], "docs", docs_batch_body);
    defer docs_batch.deinit(std.heap.page_allocator);

    const orders_batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"order:1":{"user":"doc:a","item":"legacy","qty":1}}}
    );
    defer std.heap.page_allocator.free(orders_batch_body);
    var orders_batch = try client.fetchBatch(bootstrap_api_base_uris[0], "orders", orders_batch_body);
    defer orders_batch.deinit(std.heap.page_allocator);

    var left_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:a", null);
    defer left_lookup.deinit(std.heap.page_allocator);
    var right_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:z", null);
    defer right_lookup.deinit(std.heap.page_allocator);
    var order_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "orders", "order:1", null);
    defer order_lookup.deinit(std.heap.page_allocator);

    var churn_executor = TxnTopologyChurnExecutor{
        .forward = client_executor.executor(),
        .cluster = &cluster,
        .metadata_apis = &metadata_apis,
        .mode = .merge_once,
    };
    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    try startPublicApiServersWithExecutor(
        4,
        &cluster,
        &roots,
        churn_executor.executor(),
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(client_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_left_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        left_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_left_body);
    var read_left = try client.fetchTransactionSessionRead(client_base, txn_id_hex, read_left_body);
    defer read_left.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_left.status);

    const read_right_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:z",
        right_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_right_body);
    var read_right = try client.fetchTransactionSessionRead(client_base, txn_id_hex, read_right_body);
    defer read_right.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_right.status);

    const read_order_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "orders",
        "order:1",
        order_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_order_body);
    var read_order = try client.fetchTransactionSessionRead(client_base, txn_id_hex, read_order_body);
    defer read_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_order.status);

    const write_left_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        "{\"title\":\"alpha cross session retried\",\"body\":\"left cross session retried\"}",
    );
    defer std.heap.page_allocator.free(write_left_body);
    var write_left = try client.fetchTransactionSessionWrite(client_base, txn_id_hex, write_left_body);
    defer write_left.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_left.status);

    const write_right_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:z",
        "{\"title\":\"zeta cross session retried\",\"body\":\"right cross session retried\"}",
    );
    defer std.heap.page_allocator.free(write_right_body);
    var write_right = try client.fetchTransactionSessionWrite(client_base, txn_id_hex, write_right_body);
    defer write_right.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_right.status);

    const write_order_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "orders",
        "order:1",
        "{\"user\":\"doc:a\",\"item\":\"retried\",\"qty\":2}",
    );
    defer std.heap.page_allocator.free(write_order_body);
    var write_order = try client.fetchTransactionSessionWrite(client_base, txn_id_hex, write_order_body);
    defer write_order.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_order.status);

    var committed = try client.fetchTransactionSessionCommit(client_base, txn_id_hex, "");
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try parsePageJson(transactions_api.SessionCommitResponse, committed.body);
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);
    try std.testing.expectEqual(@as(u32, 1), churn_executor.trigger_count);

    var session_info_after_commit = try client.fetchTransactionSessionInfo(client_base, txn_id_hex);
    defer session_info_after_commit.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), session_info_after_commit.status);

    var updated_left = try client.fetchLookup(client_base, "docs", "doc:a", null);
    defer updated_left.deinit(std.heap.page_allocator);
    var parsed_updated_left = try parseJsonBody(LookupTitle, updated_left.body);
    defer parsed_updated_left.deinit();
    try std.testing.expectEqualStrings("alpha cross session retried", parsed_updated_left.value.title);
    var updated_right = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer updated_right.deinit(std.heap.page_allocator);
    var parsed_updated_right = try parseJsonBody(LookupTitle, updated_right.body);
    defer parsed_updated_right.deinit();
    try std.testing.expectEqualStrings("zeta cross session retried", parsed_updated_right.value.title);
    var updated_order = try client.fetchLookup(client_base, "orders", "order:1", null);
    defer updated_order.deinit(std.heap.page_allocator);
    var parsed_updated_order = try parseJsonBody(OrderItem, updated_order.body);
    defer parsed_updated_order.deinit();
    try std.testing.expectEqualStrings("retried", parsed_updated_order.value.item);
}

test "public api multi-node e2e fails transaction session commit after repeated topology churn beyond retry limit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-session-retry-fail-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6151, root_a, cat_a),
        makeHostSimConfig(2, 6151, root_b, cat_b),
        makeHostSimConfig(3, 6151, root_c, cat_c),
        makeHostSimConfig(4, 6151, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6151, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var bootstrap_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var bootstrap_servers: [4]api_http_server.ApiHttpServer = undefined;
    var bootstrap_status_sources: [4]PublicApiStatusSource = undefined;
    var bootstrap_catalog_sources: [4]PublicApiCatalogSource = undefined;
    var bootstrap_routers: [4]PublicApiRouter(4) = undefined;
    var bootstrap_read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var bootstrap_write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var bootstrap_api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var bootstrap_forward_executor: std_http_executor.StdHttpExecutor = undefined;
    bootstrap_forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer bootstrap_forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &bootstrap_forward_executor,
        &bootstrap_listeners,
        &bootstrap_servers,
        &bootstrap_status_sources,
        &bootstrap_catalog_sources,
        &bootstrap_routers,
        &bootstrap_read_sources,
        &bootstrap_write_sources,
        &bootstrap_api_base_uris,
    );
    defer for (&bootstrap_listeners) |*listener| listener.deinit();
    defer for (bootstrap_api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node session retry fail docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(bootstrap_api_base_uris[0], "docs", create_body);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (source_group_id != 0) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":615101,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(615101)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(bootstrap_api_base_uris[0], "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    var left_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:a", null);
    defer left_lookup.deinit(std.heap.page_allocator);
    var right_lookup = try client.fetchLookup(bootstrap_api_base_uris[0], "docs", "doc:z", null);
    defer right_lookup.deinit(std.heap.page_allocator);

    var churn_executor = TxnTopologyChurnExecutor{
        .forward = client_executor.executor(),
        .cluster = &cluster,
        .metadata_apis = &metadata_apis,
        .mode = .merge_then_split,
    };
    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    try startPublicApiServersWithExecutor(
        4,
        &cluster,
        &roots,
        churn_executor.executor(),
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(std.heap.page_allocator, "write");
    defer std.heap.page_allocator.free(begin_body);
    var begin = try client.fetchTransactionBegin(client_base, begin_body);
    defer begin.deinit(std.heap.page_allocator);
    var parsed_begin = try parsePageJson(transactions_api.BeginResponse, begin.body);
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_left_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        left_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_left_body);
    var read_left = try client.fetchTransactionSessionRead(client_base, txn_id_hex, read_left_body);
    defer read_left.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_left.status);

    const read_right_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.heap.page_allocator,
        "docs",
        "doc:z",
        right_lookup.version.?,
    );
    defer std.heap.page_allocator.free(read_right_body);
    var read_right = try client.fetchTransactionSessionRead(client_base, txn_id_hex, read_right_body);
    defer read_right.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), read_right.status);

    const write_left_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:a",
        "{\"title\":\"alpha session retry fail\",\"body\":\"left session retry fail\"}",
    );
    defer std.heap.page_allocator.free(write_left_body);
    var write_left = try client.fetchTransactionSessionWrite(client_base, txn_id_hex, write_left_body);
    defer write_left.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_left.status);

    const write_right_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.heap.page_allocator,
        "docs",
        "doc:z",
        "{\"title\":\"zeta session retry fail\",\"body\":\"right session retry fail\"}",
    );
    defer std.heap.page_allocator.free(write_right_body);
    var write_right = try client.fetchTransactionSessionWrite(client_base, txn_id_hex, write_right_body);
    defer write_right.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), write_right.status);

    var aborted = try client.fetchTransactionSessionCommit(client_base, txn_id_hex, "");
    defer aborted.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 409), aborted.status);
    var parsed_abort = try parsePageJson(transactions_api.SessionCommitResponse, aborted.body);
    defer parsed_abort.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_abort.value.status);
    try std.testing.expectEqualStrings("topology_changed", parsed_abort.value.conflict.?.kind);
    try std.testing.expectEqualStrings("topology", parsed_abort.value.conflict.?.retry_scope.?);
    try std.testing.expectEqual(@as(u32, 100), parsed_abort.value.conflict.?.retry_after_ms.?);
    try std.testing.expectEqual(@as(u32, 2), churn_executor.trigger_count);

    var session_info = try client.fetchTransactionSessionInfo(client_base, txn_id_hex);
    defer session_info.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), session_info.status);
    var parsed_session_info = try parsePageJson(transactions_api.SessionDetailsResponse, session_info.body);
    defer parsed_session_info.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_session_info.value.staged_read_count);
    try std.testing.expectEqual(@as(usize, 2), parsed_session_info.value.staged_write_count);

    var abort_resp = try client.fetchTransactionAbort(client_base, txn_id_hex);
    defer abort_resp.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), abort_resp.status);

    var session_info_after_abort = try client.fetchTransactionSessionInfo(client_base, txn_id_hex);
    defer session_info_after_abort.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 404), session_info_after_abort.status);
}

test "public api multi-node e2e recovers unresolved distributed transaction after participant leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-txn-restart-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6148, root_a, cat_a),
        makeHostSimConfig(2, 6148, root_b, cat_b),
        makeHostSimConfig(3, 6148, root_c, cat_c),
        makeHostSimConfig(4, 6148, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6148, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const metadata_leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(metadata_leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(metadata_leader_index);
    try cluster.publishClusterStores(metadata_leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node txn restart docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const projected_ranges = try cluster.node(0).listProjectedRanges(std.testing.allocator);
        defer cluster.node(0).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (source_group_id != 0) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":614801,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse metadata_leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse metadata_leader_index;
        if (try cluster.node(query_index).observeSplitTransition(614801)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse metadata_leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse metadata_leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const left_leader_index = currentGroupLeaderIndex(&cluster, left_group) orelse return error.TestExpectedEqual;
    const right_leader_index = currentGroupLeaderIndex(&cluster, right_group) orelse return error.TestExpectedEqual;
    const left_participant = try distributed_txn.participantIdForGroup(std.testing.allocator, "docs", left_group);
    defer std.testing.allocator.free(left_participant);
    const right_participant = try distributed_txn.participantIdForGroup(std.testing.allocator, "docs", right_group);
    defer std.testing.allocator.free(right_participant);
    const participants = [_][]const u8{ left_participant, right_participant };
    const recovery_txn_id = try distributed_txn.parseTxnIdHex("1234567890abcdef1234567890abcdef");

    try seedHalfResolvedCommittedTxnOnGroup(
        std.testing.allocator,
        roots[left_leader_index],
        left_group,
        recovery_txn_id,
        50_000,
        50_100,
        &participants,
        left_participant,
        "doc:b",
        "{\"title\":\"left recovery pending\"}",
    );
    try seedHalfResolvedCommittedTxnOnGroup(
        std.testing.allocator,
        roots[right_leader_index],
        right_group,
        recovery_txn_id,
        50_000,
        50_100,
        &participants,
        right_participant,
        "doc:y",
        "{\"title\":\"right recovery pending\"}",
    );

    const left_restart_index = left_leader_index;
    const right_restart_index = right_leader_index;
    try cluster.restartNode(left_restart_index);
    if (right_restart_index != left_restart_index) try cluster.restartNode(right_restart_index);

    rounds = 0;
    while (rounds < 32) : (rounds += 1) try cluster.stepAll();
    try cluster.node(left_restart_index).campaignGroup(left_group);
    if (right_restart_index != left_restart_index) {
        try cluster.node(right_restart_index).campaignGroup(right_group);
    } else if (right_group != left_group) {
        try cluster.node(right_restart_index).campaignGroup(right_group);
    }

    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        if (currentGroupLeaderIndex(&cluster, left_group) == left_restart_index and currentGroupLeaderIndex(&cluster, right_group) == right_restart_index) break;
    }

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(api_base_uris[0], "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    var left_lookup = try client.fetchLookup(client_base, "docs", "doc:a", null);
    defer left_lookup.deinit(std.heap.page_allocator);
    var right_lookup = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer right_lookup.deinit(std.heap.page_allocator);

    const commit_batch = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{"doc:a":{"title":"alpha recovery","body":"left recovery"},"doc:z":{"title":"zeta recovery","body":"right recovery"}}}
    );
    defer std.heap.page_allocator.free(commit_batch);
    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.heap.page_allocator,
        &.{
            .{ .table_name = "docs", .key = "doc:a", .version = left_lookup.version.? },
            .{ .table_name = "docs", .key = "doc:z", .version = right_lookup.version.? },
        },
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        null,
    );
    defer std.heap.page_allocator.free(commit_body);

    var committed = try client.fetchTransactionCommit(client_base, commit_body);
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.heap.page_allocator, committed.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);

    try expectTxnCleanedOnGroup(std.testing.allocator, roots[left_restart_index], left_group, recovery_txn_id);
    try expectTxnCleanedOnGroup(std.testing.allocator, roots[right_restart_index], right_group, recovery_txn_id);
}

test "public api multi-node e2e routes semantic and sparse queries from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6140, root_a, cat_a),
        makeHostSimConfig(2, 6140, root_b, cat_b),
        makeHostSimConfig(3, 6140, root_c, cat_c),
        makeHostSimConfig(4, 6140, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6140, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var media_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeRemoteMedia.executor());
    defer media_listener.deinit();
    try media_listener.start();
    const media_base_uri = try media_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(media_base_uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node semantic docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.heap.page_allocator,
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
    defer std.heap.page_allocator.free(semantic_index_body);
    var semantic_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_idx", semantic_index_body);
    defer semantic_index.deinit(std.heap.page_allocator);

    const fixed_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.heap.page_allocator,
        "semantic_fixed_idx",
        "body",
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
    defer std.heap.page_allocator.free(fixed_chunked_index_body);
    var fixed_chunked_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_fixed_idx", fixed_chunked_index_body);
    defer fixed_chunked_index.deinit(std.heap.page_allocator);

    const antfly_chunk_api = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/api", .{antfly_base_uri});
    defer std.heap.page_allocator.free(antfly_chunk_api);
    const antfly_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.heap.page_allocator,
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
    defer std.heap.page_allocator.free(antfly_chunked_index_body);
    var antfly_chunked_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_antfly_idx", antfly_chunked_index_body);
    defer antfly_chunked_index.deinit(std.heap.page_allocator);

    const semantic_template_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexTemplateRequest(
        std.heap.page_allocator,
        "semantic_template_idx",
        "{{remoteMedia url=photo}}",
        3,
        .{
            .provider = .antfly,
            .model = "antfly-clip-v1",
            .api_url = antfly_base_uri,
            .multimodal = true,
        },
    );
    defer std.heap.page_allocator.free(semantic_template_index_body);
    var semantic_template_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_template_idx", semantic_template_index_body);
    defer semantic_template_index.deinit(std.heap.page_allocator);

    const semantic_template_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexTemplateWithChunkerRequest(
        std.heap.page_allocator,
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
    defer std.heap.page_allocator.free(semantic_template_chunked_index_body);
    var semantic_template_chunked_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_template_chunked_idx", semantic_template_chunked_index_body);
    defer semantic_template_chunked_index.deinit(std.heap.page_allocator);

    const sparse_index_body = try test_contract_helpers.encodeManagedSparseEmbeddingsIndexRequest(
        std.heap.page_allocator,
        "sparse_idx",
        "body",
        .{
            .provider = .antfly,
            .model = "antfly-sparse-v1",
            .api_url = antfly_base_uri,
        },
    );
    defer std.heap.page_allocator.free(sparse_index_body);
    var sparse_index = try client.createTableIndex(api_base_uris[0], "docs", "sparse_idx", sparse_index_body);
    defer sparse_index.deinit(std.heap.page_allocator);

    var group_id: u64 = 0;
    var client_index: ?usize = null;
    var rounds: usize = 0;
    while (rounds < 96) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        group_id = projected_ranges[0].group_id;

        var active_count: usize = 0;
        client_index = null;
        for (0..4) |i| {
            if (cluster.node(i).status(group_id) == .active) {
                active_count += 1;
            } else {
                client_index = i;
            }
        }
        if (active_count == 3 and client_index != null) break;
    }
    try std.testing.expect(group_id != 0);
    const routed_client_index = client_index orelse return error.TestExpectedEqual;
    const client_base = api_base_uris[routed_client_index];

    const group_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 96) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, group_id);
            if (found != null) break;
            try cluster.stepAll();
        }
        if (found == null) {
            for (0..4) |i| {
                if (cluster.node(i).status(group_id) == .active) {
                    found = i;
                    break;
                }
            }
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    try ensureGroupEmbeddingIndexes(&cluster, roots[group_leader_index], group_id, "semantic_idx", "sparse_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[group_leader_index], group_id, "semantic_fixed_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[group_leader_index], group_id, "semantic_antfly_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[group_leader_index], group_id, "semantic_template_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[group_leader_index], group_id, "semantic_template_chunked_idx", 64);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"alpha body","photo":"MEDIA_URL_A","transcript":"TEXT_URL_A"},
        \\  "doc:b":{"title":"beta","body":"beta body","photo":"MEDIA_URL_B","transcript":"TEXT_URL_B"}
        \\}}
    );
    defer std.heap.page_allocator.free(batch_body);
    const media_url_doc_a = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/kitten.png", .{media_base_uri});
    defer std.heap.page_allocator.free(media_url_doc_a);
    const media_url_doc_b = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/doc.txt", .{media_base_uri});
    defer std.heap.page_allocator.free(media_url_doc_b);
    const text_url_doc_a = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/doc.txt", .{media_base_uri});
    defer std.heap.page_allocator.free(text_url_doc_a);
    const text_url_doc_b = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/doc.txt", .{media_base_uri});
    defer std.heap.page_allocator.free(text_url_doc_b);
    const batch_body_replaced = try std.mem.replaceOwned(u8, std.heap.page_allocator, batch_body, "MEDIA_URL_A", media_url_doc_a);
    defer std.heap.page_allocator.free(batch_body_replaced);
    const batch_body_with_b = try std.mem.replaceOwned(u8, std.heap.page_allocator, batch_body_replaced, "MEDIA_URL_B", media_url_doc_b);
    defer std.heap.page_allocator.free(batch_body_with_b);
    const batch_body_with_text_a = try std.mem.replaceOwned(u8, std.heap.page_allocator, batch_body_with_b, "TEXT_URL_A", text_url_doc_a);
    defer std.heap.page_allocator.free(batch_body_with_text_a);
    const final_batch_body = try std.mem.replaceOwned(u8, std.heap.page_allocator, batch_body_with_text_a, "TEXT_URL_B", text_url_doc_b);
    defer std.heap.page_allocator.free(final_batch_body);
    var batch = try client.fetchBatch(client_base, "docs", final_batch_body);
    defer batch.deinit(std.heap.page_allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_batch.value.inserted.?);

    const semantic_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_idx"}, 5);
    defer std.heap.page_allocator.free(semantic_query_body);
    var semantic_query = try client.fetchQuery(client_base, "docs", semantic_query_body);
    defer semantic_query.deinit(std.heap.page_allocator);
    var parsed_semantic = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, semantic_query.body, .{});
    defer parsed_semantic.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_semantic.value.responses.?[0].hits.?.hits.?[0]._id);

    const fixed_chunked_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_fixed_idx"}, 5);
    defer std.heap.page_allocator.free(fixed_chunked_query_body);
    var fixed_chunked_query = try client.fetchQuery(client_base, "docs", fixed_chunked_query_body);
    defer fixed_chunked_query.deinit(std.heap.page_allocator);
    var parsed_fixed_chunked = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, fixed_chunked_query.body, .{});
    defer parsed_fixed_chunked.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_fixed_chunked.value.responses.?[0].hits.?.hits.?[0]._id);

    const antfly_chunked_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_antfly_idx"}, 5);
    defer std.heap.page_allocator.free(antfly_chunked_query_body);
    var antfly_chunked_query = try client.fetchQuery(client_base, "docs", antfly_chunked_query_body);
    defer antfly_chunked_query.deinit(std.heap.page_allocator);
    var parsed_antfly_chunked = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, antfly_chunked_query.body, .{});
    defer parsed_antfly_chunked.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_antfly_chunked.value.responses.?[0].hits.?.hits.?[0]._id);

    const sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(std.heap.page_allocator, "sparse_idx", &.{ 7, 42 }, &.{ 1.5, 0.5 }, 5);
    defer std.heap.page_allocator.free(sparse_query_body);
    var sparse_query = try client.fetchQuery(client_base, "docs", sparse_query_body);
    defer sparse_query.deinit(std.heap.page_allocator);
    var parsed_sparse = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, sparse_query.body, .{});
    defer parsed_sparse.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_sparse.value.responses.?[0].hits.?.hits.?[0]._id);

    const media_url = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/kitten.png", .{media_base_uri});
    defer std.heap.page_allocator.free(media_url);
    const templated_query_body = try test_contract_helpers.encodeSemanticQueryWithTemplateRequest(
        std.heap.page_allocator,
        media_url,
        "{{remoteMedia url=this}}",
        &.{"semantic_antfly_idx"},
        5,
    );
    defer std.heap.page_allocator.free(templated_query_body);
    var templated_query = try client.fetchQuery(client_base, "docs", templated_query_body);
    defer templated_query.deinit(std.heap.page_allocator);
    var parsed_templated = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, templated_query.body, .{});
    defer parsed_templated.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_templated.value.responses.?[0].hits.?.hits.?[0]._id);

    const template_index_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_template_idx"}, 5);
    defer std.heap.page_allocator.free(template_index_query_body);
    var template_index_query = try client.fetchQuery(client_base, "docs", template_index_query_body);
    defer template_index_query.deinit(std.heap.page_allocator);
    var parsed_template_index = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, template_index_query.body, .{});
    defer parsed_template_index.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_template_index.value.responses.?[0].hits.?.hits.?[0]._id);

    const template_chunked_query_body = try test_contract_helpers.encodeSemanticQueryWithTemplateRequest(
        std.heap.page_allocator,
        text_url_doc_a,
        "{{remoteText url=this}}",
        &.{"semantic_template_chunked_idx"},
        5,
    );
    defer std.heap.page_allocator.free(template_chunked_query_body);
    var template_chunked_query = try client.fetchQuery(client_base, "docs", template_chunked_query_body);
    defer template_chunked_query.deinit(std.heap.page_allocator);
    var parsed_template_chunked = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, template_chunked_query.body, .{});
    defer parsed_template_chunked.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_template_chunked.value.responses.?[0].hits.?.hits.?[0]._id);
}

test "public api multi-node e2e routes graph queries from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-graph-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6170, root_a, cat_a),
        makeHostSimConfig(2, 6170, root_b, cat_b),
        makeHostSimConfig(3, 6170, root_c, cat_c),
        makeHostSimConfig(4, 6170, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6170, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(
        4,
        &cluster,
        &metadata_admin_listeners,
        &metadata_admin_servers,
        &metadata_admin_sources,
        &metadata_apis,
    );
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node graph docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var graph_index = try client.createTableIndex(api_base_uris[0], "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index.deinit(std.heap.page_allocator);

    var group_id: u64 = 0;
    var client_index: ?usize = null;
    var rounds: usize = 0;
    while (rounds < 96) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        group_id = projected_ranges[0].group_id;

        var active_count: usize = 0;
        client_index = null;
        for (0..4) |i| {
            if (cluster.node(i).status(group_id) == .active) {
                active_count += 1;
            } else {
                client_index = i;
            }
        }
        if (active_count == 3 and client_index != null) break;
    }
    try std.testing.expect(group_id != 0);
    const routed_client_index = client_index orelse return error.TestExpectedEqual;
    const client_base = api_base_uris[routed_client_index];

    const group_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 96) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, group_id);
            if (found != null) break;
            try cluster.stepAll();
        }
        if (found == null) {
            for (0..4) |i| {
                if (cluster.node(i).status(group_id) == .active) {
                    found = i;
                    break;
                }
            }
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    try ensureGroupGraphIndex(&cluster, roots[group_leader_index], group_id, "graph_idx", 64);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","_edges":{"graph_idx":{"links":[{"target":"doc:b"}]}}},
        \\  "doc:b":{"title":"beta"},
        \\  "doc:c":{"title":"gamma"}
        \\}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(client_base, "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 3), parsed_batch.value.inserted.?);

    const graph_query_body = try test_contract_helpers.encodeGraphNeighborsQueryRequest(
        std.heap.page_allocator,
        "neighbors",
        "graph_idx",
        &.{"doc:a"},
        &.{"links"},
        10,
    );
    defer std.heap.page_allocator.free(graph_query_body);
    var graph_query = try client.fetchQuery(client_base, "docs", graph_query_body);
    defer graph_query.deinit(std.heap.page_allocator);
    var parsed_graph = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, graph_query.body, .{});
    defer parsed_graph.deinit();
    const graph_results = parsed_graph.value.responses.?[0].graph_results.?;
    const neighbors = graph_results.map.get("neighbors").?;
    try std.testing.expectEqual(indexes_openapi.GraphQueryType.neighbors, neighbors.type);
    try std.testing.expectEqual(@as(i64, 1), neighbors.total);
    try std.testing.expectEqualStrings("doc:b", neighbors.nodes.?[0].key);
}

test "public api multi-node e2e routes split flow from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-split-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6120, root_a, cat_a),
        makeHostSimConfig(2, 6120, root_b, cat_b),
        makeHostSimConfig(3, 6120, root_c, cat_c),
        makeHostSimConfig(4, 6120, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6120, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(
        4,
        &cluster,
        &metadata_admin_listeners,
        &metadata_admin_servers,
        &metadata_admin_sources,
        &metadata_apis,
    );
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node split docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var graph_index = try client.createTableIndex(api_base_uris[0], "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index.deinit(std.heap.page_allocator);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;

        var active_count: usize = 0;
        for (0..4) |i| {
            if (cluster.node(i).status(source_group_id) == .active) active_count += 1;
        }
        if (active_count == 3) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":612001,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        split_body,
    );

    var finalized = false;
    rounds = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(612001)) |observation| {
            if (observation.status.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var client_index: ?usize = null;
    rounds = 0;
    while (rounds < 48) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                client_index = i;
                break;
            }
        }
        if (client_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const left_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 24) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, left_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    const right_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 24) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, right_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    try ensureGroupTextIndex(&cluster, roots[left_leader_index], left_group, "full_text_index_v0", 40);
    try ensureGroupTextIndex(&cluster, roots[right_leader_index], right_group, "full_text_index_v0", 40);
    try ensureGroupGraphIndex(&cluster, roots[left_leader_index], left_group, "graph_idx", 40);
    try ensureGroupGraphIndex(&cluster, roots[right_leader_index], right_group, "graph_idx", 40);

    const client_base = api_base_uris[client_index orelse return error.TestExpectedEqual];
    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"hello left side","_edges":{"graph_idx":{"links":[{"target":"doc:z"}]}}},
        \\  "doc:b":{"title":"beta","body":"left anchor"},
        \\  "doc:z":{"title":"zeta","body":"hello right side","_edges":{"graph_idx":{"links":[{"target":"doc:y"}]}}},
        \\  "doc:y":{"title":"yotta","body":"right neighbor"}
        \\}}
    );
    defer std.heap.page_allocator.free(batch_body);
    var batch = try client.fetchBatch(client_base, "docs", batch_body);
    defer batch.deinit(std.heap.page_allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_batch.value.inserted.?);

    const query_body = try test_contract_helpers.encodeMatchQueryRequestWithFlags(
        std.heap.page_allocator,
        "body",
        "hello",
        &.{},
        10,
        true,
        true,
    );
    defer std.heap.page_allocator.free(query_body);
    var query = try client.fetchQuery(client_base, "docs", query_body);
    defer query.deinit(std.heap.page_allocator);
    var query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, query.body, .{});
    defer query_responses.deinit();
    const query_result = query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 4), query_result.hits.?.total.?);
    try expectQueryProfileSummary(std.heap.page_allocator, query_result.profile, 2, true);

    const graph_query_body = try test_contract_helpers.encodeGraphTraverseQueryRequest(
        std.heap.page_allocator,
        "walk",
        "graph_idx",
        &.{"doc:a"},
        &.{"links"},
        2,
        10,
    );
    defer std.heap.page_allocator.free(graph_query_body);
    var graph_query = try client.fetchQuery(client_base, "docs", graph_query_body);
    defer graph_query.deinit(std.heap.page_allocator);
    var graph_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, graph_query.body, .{});
    defer graph_responses.deinit();
    const graph_result = graph_responses.value.responses.?[0].graph_results.?.map.get("walk").?;
    try std.testing.expectEqual(@as(i64, 2), graph_result.total);
    try expectGraphNodeKeys(graph_result.nodes, &.{ "doc:z", "doc:y" });

    const graph_paths_query_body = try test_contract_helpers.encodeGraphTraverseQueryRequestWithPaths(
        std.heap.page_allocator,
        "walk_paths",
        "graph_idx",
        &.{"doc:a"},
        &.{"links"},
        2,
        10,
    );
    defer std.heap.page_allocator.free(graph_paths_query_body);
    var graph_paths_query = try client.fetchQuery(client_base, "docs", graph_paths_query_body);
    defer graph_paths_query.deinit(std.heap.page_allocator);
    var graph_paths_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, graph_paths_query.body, .{});
    defer graph_paths_responses.deinit();
    const graph_paths_result = graph_paths_responses.value.responses.?[0].graph_results.?.map.get("walk_paths").?;
    try std.testing.expectEqual(@as(i64, 2), graph_paths_result.total);
    try expectGraphNodePath(graph_paths_result.nodes, "doc:z", &.{ "doc:a", "doc:z" }, &.{"links"});
    try expectGraphNodePath(graph_paths_result.nodes, "doc:y", &.{ "doc:a", "doc:z", "doc:y" }, &.{ "links", "links" });

    const shortest_query_body = try test_contract_helpers.encodeGraphShortestPathQueryRequest(
        std.heap.page_allocator,
        "shortest",
        "graph_idx",
        &.{"doc:a"},
        &.{"doc:y"},
        &.{"links"},
        4,
        10,
    );
    defer std.heap.page_allocator.free(shortest_query_body);
    var shortest_query = try client.fetchQuery(client_base, "docs", shortest_query_body);
    defer shortest_query.deinit(std.heap.page_allocator);
    var shortest_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, shortest_query.body, .{});
    defer shortest_responses.deinit();
    const shortest_result = shortest_responses.value.responses.?[0].graph_results.?.map.get("shortest").?;
    try std.testing.expectEqual(@as(i64, 1), shortest_result.total);
    try expectGraphNodeKeys(shortest_result.nodes, &.{"doc:y"});
    try expectGraphNodePath(shortest_result.nodes, "doc:y", &.{ "doc:a", "doc:z", "doc:y" }, &.{ "links", "links" });

    const weighted_batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"hello left side","_edges":{"graph_idx":{"links":[{"target":"doc:z","weight":0.9},{"target":"doc:b","weight":0.2}]}}},
        \\  "doc:b":{"title":"beta","body":"left anchor","_edges":{"graph_idx":{"links":[{"target":"doc:c","weight":0.2}]}}},
        \\  "doc:c":{"title":"gamma","body":"weighted left chain","_edges":{"graph_idx":{"links":[{"target":"doc:y","weight":0.2}]}}},
        \\  "doc:z":{"title":"zeta","body":"hello right side","_edges":{"graph_idx":{"links":[{"target":"doc:y","weight":0.9}]}}},
        \\  "doc:y":{"title":"yotta","body":"right neighbor"}
        \\}}
    );
    defer std.heap.page_allocator.free(weighted_batch_body);
    var weighted_batch = try client.fetchBatch(client_base, "docs", weighted_batch_body);
    defer weighted_batch.deinit(std.heap.page_allocator);
    var weighted_batch_res = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, weighted_batch.body, .{});
    defer weighted_batch_res.deinit();
    try std.testing.expectEqual(@as(i64, 5), weighted_batch_res.value.inserted.?);

    const min_weight_query_body = try test_contract_helpers.encodeWeightedGraphShortestPathQueryRequest(
        std.heap.page_allocator,
        "shortest_min_weight",
        "graph_idx",
        &.{"doc:a"},
        &.{"doc:y"},
        &.{"links"},
        5,
        10,
        .min_weight,
    );
    defer std.heap.page_allocator.free(min_weight_query_body);
    var min_weight_query = try client.fetchQuery(client_base, "docs", min_weight_query_body);
    defer min_weight_query.deinit(std.heap.page_allocator);
    var min_weight_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, min_weight_query.body, .{});
    defer min_weight_responses.deinit();
    const min_weight_result = min_weight_responses.value.responses.?[0].graph_results.?.map.get("shortest_min_weight").?;
    try std.testing.expectEqual(@as(i64, 1), min_weight_result.total);
    try expectGraphNodePath(min_weight_result.nodes, "doc:y", &.{ "doc:a", "doc:b", "doc:c", "doc:y" }, &.{ "links", "links", "links" });

    const max_weight_query_body = try test_contract_helpers.encodeWeightedGraphShortestPathQueryRequest(
        std.heap.page_allocator,
        "shortest_max_weight",
        "graph_idx",
        &.{"doc:a"},
        &.{"doc:y"},
        &.{"links"},
        5,
        10,
        .max_weight,
    );
    defer std.heap.page_allocator.free(max_weight_query_body);
    var max_weight_query = try client.fetchQuery(client_base, "docs", max_weight_query_body);
    defer max_weight_query.deinit(std.heap.page_allocator);
    var max_weight_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, max_weight_query.body, .{});
    defer max_weight_responses.deinit();
    const max_weight_result = max_weight_responses.value.responses.?[0].graph_results.?.map.get("shortest_max_weight").?;
    try std.testing.expectEqual(@as(i64, 1), max_weight_result.total);
    try expectGraphNodePath(max_weight_result.nodes, "doc:y", &.{ "doc:a", "doc:z", "doc:y" }, &.{ "links", "links" });

    const k_shortest_query_body = try test_contract_helpers.encodeWeightedGraphKShortestPathsQueryRequest(
        std.heap.page_allocator,
        "k_shortest",
        "graph_idx",
        &.{"doc:a"},
        &.{"doc:y"},
        &.{"links"},
        5,
        10,
        2,
        .min_weight,
    );
    defer std.heap.page_allocator.free(k_shortest_query_body);
    var k_shortest_query = try client.fetchQuery(client_base, "docs", k_shortest_query_body);
    defer k_shortest_query.deinit(std.heap.page_allocator);
    var k_shortest_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, k_shortest_query.body, .{});
    defer k_shortest_responses.deinit();
    const k_shortest_result = k_shortest_responses.value.responses.?[0].graph_results.?.map.get("k_shortest").?;
    try std.testing.expectEqual(@as(i64, 2), k_shortest_result.total);
    try expectGraphNodePath(k_shortest_result.nodes, "doc:y", &.{ "doc:a", "doc:b", "doc:c", "doc:y" }, &.{ "links", "links", "links" });
    const k_shortest_nodes = k_shortest_result.nodes orelse return error.TestExpectedEqual;
    try expectGraphNodePath(k_shortest_nodes[1..], "doc:y", &.{ "doc:a", "doc:z", "doc:y" }, &.{ "links", "links" });

    const ref_graph_query_body = try test_contract_helpers.encodeMatchGraphTraverseFromResultRefQueryRequest(
        std.heap.page_allocator,
        "title",
        "alpha",
        "walk_from_text",
        "graph_idx",
        "$full_text_results",
        2,
        10,
    );
    defer std.heap.page_allocator.free(ref_graph_query_body);
    var ref_graph_query = try client.fetchQuery(client_base, "docs", ref_graph_query_body);
    defer ref_graph_query.deinit(std.heap.page_allocator);
    var ref_graph_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, ref_graph_query.body, .{});
    defer ref_graph_responses.deinit();
    const ref_query_result = ref_graph_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), ref_query_result.hits.?.total.?);
    const ref_graph_result = ref_query_result.graph_results.?.map.get("walk_from_text").?;
    try std.testing.expectEqual(@as(i64, 2), ref_graph_result.total);
    try expectGraphNodeKeys(ref_graph_result.nodes, &.{ "doc:z", "doc:y" });

    const fused_ref_graph_query_body = try test_contract_helpers.encodeMatchGraphTraverseFromResultRefQueryRequest(
        std.heap.page_allocator,
        "title",
        "alpha",
        "walk_from_fused",
        "graph_idx",
        "$fused_results",
        2,
        10,
    );
    defer std.heap.page_allocator.free(fused_ref_graph_query_body);
    var fused_ref_graph_query = try client.fetchQuery(client_base, "docs", fused_ref_graph_query_body);
    defer fused_ref_graph_query.deinit(std.heap.page_allocator);
    var fused_ref_graph_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, fused_ref_graph_query.body, .{});
    defer fused_ref_graph_responses.deinit();
    const fused_ref_query_result = fused_ref_graph_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 1), fused_ref_query_result.hits.?.total.?);
    const fused_ref_graph_result = fused_ref_query_result.graph_results.?.map.get("walk_from_fused").?;
    try std.testing.expectEqual(@as(i64, 2), fused_ref_graph_result.total);
    try expectGraphNodeKeys(fused_ref_graph_result.nodes, &.{ "doc:z", "doc:y" });

    var lookup = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer lookup.deinit(std.heap.page_allocator);
    var parsed_lookup = try parseJsonBody(LookupTitle, lookup.body);
    defer parsed_lookup.deinit();
    try std.testing.expectEqualStrings("zeta", parsed_lookup.value.title);

    const delete_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator, "{\"deletes\":[\"doc:z\"]}");
    defer std.heap.page_allocator.free(delete_body);
    var deleted = try client.fetchBatch(client_base, "docs", delete_body);
    defer deleted.deinit(std.heap.page_allocator);
    var parsed_deleted = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, deleted.body, .{});
    defer parsed_deleted.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_deleted.value.deleted.?);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:z", null));
}

test "public api multi-node e2e routes merge flow from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-merge-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6130, root_a, cat_a),
        makeHostSimConfig(2, 6130, root_b, cat_b),
        makeHostSimConfig(3, 6130, root_c, cat_c),
        makeHostSimConfig(4, 6130, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6130, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(
        4,
        &cluster,
        &metadata_admin_listeners,
        &metadata_admin_servers,
        &metadata_admin_sources,
        &metadata_apis,
    );
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node merge docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    var graph_index = try client.createTableIndex(api_base_uris[0], "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index.deinit(std.heap.page_allocator);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (projected_ranges.len == 1) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":613001,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        split_body,
    );

    var split_finalized = false;
    rounds = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(613001)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    rounds = 0;
    while (rounds < 48) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group != 0 and right_group != 0 and left_group != right_group) break;
        try cluster.stepAll();
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const left_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 24) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, left_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    const right_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 24) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, right_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    try ensureGroupTextIndex(&cluster, roots[left_leader_index], left_group, "full_text_index_v0", 40);
    try ensureGroupTextIndex(&cluster, roots[right_leader_index], right_group, "full_text_index_v0", 40);
    try ensureGroupGraphIndex(&cluster, roots[left_leader_index], left_group, "graph_idx", 40);
    try ensureGroupGraphIndex(&cluster, roots[right_leader_index], right_group, "graph_idx", 40);

    const pre_merge_batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"hello left side","_edges":{"graph_idx":{"links":[{"target":"doc:b"}]}}},
        \\  "doc:b":{"title":"beta","body":"left neighbor"},
        \\  "doc:z":{"title":"zeta","body":"hello right side","_edges":{"graph_idx":{"links":[{"target":"doc:y"}]}}},
        \\  "doc:y":{"title":"yotta","body":"right neighbor"}
        \\}}
    );
    defer std.heap.page_allocator.free(pre_merge_batch_body);
    var pre_merge_batch = try client.fetchBatch(api_base_uris[0], "docs", pre_merge_batch_body);
    defer pre_merge_batch.deinit(std.heap.page_allocator);
    var parsed_pre_merge_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, pre_merge_batch.body, .{});
    defer parsed_pre_merge_batch.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_pre_merge_batch.value.inserted.?);

    const merge_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":613002,\"donor_group_id\":{d},\"receiver_group_id\":{d}}}", .{
        right_group,
        left_group,
    });
    defer std.testing.allocator.free(merge_body);
    try metadata_client.requestTableMerge(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        merge_body,
    );

    var merge_finalized = false;
    rounds = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeMergeTransition(613002)) |observation| {
            if (observation.receiver.phase == .finalized) {
                merge_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(merge_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var client_index: ?usize = null;
    rounds = 0;
    while (rounds < 48) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const merged_group = try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:z");
        if (merged_group == left_group) {
            for (0..4) |i| {
                if (cluster.node(i).status(left_group) != .active) {
                    client_index = i;
                    break;
                }
            }
            if (client_index != null) break;
        }
        try cluster.stepAll();
    }
    const client_base = api_base_uris[client_index orelse return error.TestExpectedEqual];

    const merged_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 24) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, left_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    try ensureGroupGraphIndex(&cluster, roots[merged_leader_index], left_group, "graph_idx", 40);

    const query_body = try test_contract_helpers.encodeMatchQueryRequestWithFlags(
        std.heap.page_allocator,
        "body",
        "hello",
        &.{},
        10,
        true,
        true,
    );
    defer std.heap.page_allocator.free(query_body);
    var query = try client.fetchQuery(client_base, "docs", query_body);
    defer query.deinit(std.heap.page_allocator);
    var query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, query.body, .{});
    defer query_responses.deinit();
    const query_result = query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 4), query_result.hits.?.total.?);
    try expectQueryProfileSummary(std.heap.page_allocator, query_result.profile, 1, false);

    const graph_query_body = try test_contract_helpers.encodeGraphNeighborsQueryRequest(
        std.heap.page_allocator,
        "neighbors",
        "graph_idx",
        &.{ "doc:a", "doc:z" },
        &.{"links"},
        10,
    );
    defer std.heap.page_allocator.free(graph_query_body);
    var graph_query = try client.fetchQuery(client_base, "docs", graph_query_body);
    defer graph_query.deinit(std.heap.page_allocator);
    var graph_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, graph_query.body, .{});
    defer graph_responses.deinit();
    const graph_result = graph_responses.value.responses.?[0].graph_results.?.map.get("neighbors").?;
    try std.testing.expectEqual(@as(i64, 2), graph_result.total);
    try expectGraphNodeKeys(graph_result.nodes, &.{ "doc:b", "doc:y" });

    const delete_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator, "{\"deletes\":[\"doc:z\"]}");
    defer std.heap.page_allocator.free(delete_body);
    var deleted = try client.fetchBatch(client_base, "docs", delete_body);
    defer deleted.deinit(std.heap.page_allocator);
    var parsed_deleted = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, deleted.body, .{});
    defer parsed_deleted.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_deleted.value.deleted.?);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:z", null));
}

test "public api multi-node e2e retries distributed graph after merge churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6190, root_a, cat_a),
        makeHostSimConfig(2, 6190, root_b, cat_b),
        makeHostSimConfig(3, 6190, root_c, cat_c),
        makeHostSimConfig(4, 6190, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6190, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api distributed graph retry docs");
    defer std.heap.page_allocator.free(create_body);

    var bootstrap_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var bootstrap_servers: [4]api_http_server.ApiHttpServer = undefined;
    var bootstrap_status_sources: [4]PublicApiStatusSource = undefined;
    var bootstrap_catalog_sources: [4]PublicApiCatalogSource = undefined;
    var bootstrap_routers: [4]PublicApiRouter(4) = undefined;
    var bootstrap_read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var bootstrap_write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var bootstrap_api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var bootstrap_forward_executor: std_http_executor.StdHttpExecutor = undefined;
    bootstrap_forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer bootstrap_forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &bootstrap_forward_executor,
        &bootstrap_listeners,
        &bootstrap_servers,
        &bootstrap_status_sources,
        &bootstrap_catalog_sources,
        &bootstrap_routers,
        &bootstrap_read_sources,
        &bootstrap_write_sources,
        &bootstrap_api_base_uris,
    );
    defer for (&bootstrap_listeners) |*listener| listener.deinit();
    defer for (bootstrap_api_base_uris) |uri| std.testing.allocator.free(uri);

    _ = try client.createTable(bootstrap_api_base_uris[0], "docs", create_body);
    var graph_index = try client.createTableIndex(bootstrap_api_base_uris[0], "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index.deinit(std.heap.page_allocator);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (cluster.node(0).status(source_group_id) == .active or cluster.node(1).status(source_group_id) == .active or cluster.node(2).status(source_group_id) == .active) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":619001,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(619001)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const left_leader_index = currentGroupLeaderIndex(&cluster, left_group) orelse return error.TestExpectedEqual;
    const right_leader_index = currentGroupLeaderIndex(&cluster, right_group) orelse return error.TestExpectedEqual;
    try ensureGroupGraphIndex(&cluster, roots[left_leader_index], left_group, "graph_idx", 40);
    try ensureGroupGraphIndex(&cluster, roots[right_leader_index], right_group, "graph_idx", 40);

    const pre_query_batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"hello left side","_edges":{"graph_idx":{"links":[{"target":"doc:z"}]}}},
        \\  "doc:z":{"title":"zeta","body":"hello right side","_edges":{"graph_idx":{"links":[{"target":"doc:y"}]}}},
        \\  "doc:y":{"title":"yotta","body":"right neighbor"}
        \\}}
    );
    defer std.heap.page_allocator.free(pre_query_batch_body);
    var pre_query_batch = try client.fetchBatch(bootstrap_api_base_uris[0], "docs", pre_query_batch_body);
    defer pre_query_batch.deinit(std.heap.page_allocator);

    var churn_executor = GraphTopologyChurnExecutor{
        .forward = client_executor.executor(),
        .cluster = &cluster,
        .metadata_apis = &metadata_apis,
        .mode = .merge_once,
    };
    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    try startPublicApiServersWithExecutor(
        4,
        &cluster,
        &roots,
        churn_executor.executor(),
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const graph_query_body = try test_contract_helpers.encodeGraphTraverseQueryRequest(
        std.heap.page_allocator,
        "walk",
        "graph_idx",
        &.{"doc:a"},
        &.{"links"},
        2,
        10,
    );
    defer std.heap.page_allocator.free(graph_query_body);
    var graph_query = try client.fetchQuery(client_base, "docs", graph_query_body);
    defer graph_query.deinit(std.heap.page_allocator);
    var graph_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, graph_query.body, .{});
    defer graph_responses.deinit();
    const graph_result = graph_responses.value.responses.?[0].graph_results.?.map.get("walk").?;
    try std.testing.expectEqual(@as(i64, 2), graph_result.total);
    try expectGraphNodeKeys(graph_result.nodes, &.{ "doc:z", "doc:y" });
    try std.testing.expectEqual(@as(u32, 1), churn_executor.trigger_count);
}

test "public api multi-node e2e fails distributed graph after repeated churn beyond retry limit" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-graph-retry-fail-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6191, root_a, cat_a),
        makeHostSimConfig(2, 6191, root_b, cat_b),
        makeHostSimConfig(3, 6191, root_c, cat_c),
        makeHostSimConfig(4, 6191, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6191, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api distributed graph retry fail docs");
    defer std.heap.page_allocator.free(create_body);

    var bootstrap_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var bootstrap_servers: [4]api_http_server.ApiHttpServer = undefined;
    var bootstrap_status_sources: [4]PublicApiStatusSource = undefined;
    var bootstrap_catalog_sources: [4]PublicApiCatalogSource = undefined;
    var bootstrap_routers: [4]PublicApiRouter(4) = undefined;
    var bootstrap_read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var bootstrap_write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var bootstrap_api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var bootstrap_forward_executor: std_http_executor.StdHttpExecutor = undefined;
    bootstrap_forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer bootstrap_forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &bootstrap_forward_executor,
        &bootstrap_listeners,
        &bootstrap_servers,
        &bootstrap_status_sources,
        &bootstrap_catalog_sources,
        &bootstrap_routers,
        &bootstrap_read_sources,
        &bootstrap_write_sources,
        &bootstrap_api_base_uris,
    );
    defer for (&bootstrap_listeners) |*listener| listener.deinit();
    defer for (bootstrap_api_base_uris) |uri| std.testing.allocator.free(uri);

    _ = try client.createTable(bootstrap_api_base_uris[0], "docs", create_body);
    var graph_index = try client.createTableIndex(bootstrap_api_base_uris[0], "docs", "graph_idx", "{\"name\":\"graph_idx\",\"type\":\"graph\"}");
    defer graph_index.deinit(std.heap.page_allocator);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 48) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (source_group_id != 0) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":619101,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index], "docs", split_body);

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(619101)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var non_host_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, bootstrap_catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                non_host_index = i;
                break;
            }
        }
        if (non_host_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);

    const left_leader_index = currentGroupLeaderIndex(&cluster, left_group) orelse return error.TestExpectedEqual;
    const right_leader_index = currentGroupLeaderIndex(&cluster, right_group) orelse return error.TestExpectedEqual;
    try ensureGroupGraphIndex(&cluster, roots[left_leader_index], left_group, "graph_idx", 40);
    try ensureGroupGraphIndex(&cluster, roots[right_leader_index], right_group, "graph_idx", 40);

    const pre_query_batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"hello left side","_edges":{"graph_idx":{"links":[{"target":"doc:z"}]}}},
        \\  "doc:z":{"title":"zeta","body":"hello right side","_edges":{"graph_idx":{"links":[{"target":"doc:y"}]}}},
        \\  "doc:y":{"title":"yotta","body":"right neighbor"}
        \\}}
    );
    defer std.heap.page_allocator.free(pre_query_batch_body);
    var pre_query_batch = try client.fetchBatch(bootstrap_api_base_uris[0], "docs", pre_query_batch_body);
    defer pre_query_batch.deinit(std.heap.page_allocator);

    var churn_executor = GraphTopologyChurnExecutor{
        .forward = client_executor.executor(),
        .cluster = &cluster,
        .metadata_apis = &metadata_apis,
        .mode = .merge_then_split,
    };
    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    try startPublicApiServersWithExecutor(
        4,
        &cluster,
        &roots,
        churn_executor.executor(),
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    const client_base = api_base_uris[non_host_index orelse return error.TestExpectedEqual];
    const graph_query_body = try test_contract_helpers.encodeGraphTraverseQueryRequest(
        std.heap.page_allocator,
        "walk",
        "graph_idx",
        &.{"doc:a"},
        &.{"links"},
        2,
        10,
    );
    defer std.heap.page_allocator.free(graph_query_body);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchQuery(client_base, "docs", graph_query_body));
    try std.testing.expectEqual(@as(u32, 2), churn_executor.trigger_count);
}

test "public api multi-node e2e routes semantic and sparse queries across split ranges from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-split-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6150, root_a, cat_a),
        makeHostSimConfig(2, 6150, root_b, cat_b),
        makeHostSimConfig(3, 6150, root_c, cat_c),
        makeHostSimConfig(4, 6150, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6150, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var media_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeRemoteMedia.executor());
    defer media_listener.deinit();
    try media_listener.start();
    const media_base_uri = try media_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(media_base_uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node semantic split docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.heap.page_allocator,
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
    defer std.heap.page_allocator.free(semantic_index_body);
    var semantic_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_idx", semantic_index_body);
    defer semantic_index.deinit(std.heap.page_allocator);

    const fixed_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.heap.page_allocator,
        "semantic_fixed_idx",
        "body",
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
    defer std.heap.page_allocator.free(fixed_chunked_index_body);
    var fixed_chunked_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_fixed_idx", fixed_chunked_index_body);
    defer fixed_chunked_index.deinit(std.heap.page_allocator);

    const sparse_index_body = try test_contract_helpers.encodeManagedSparseEmbeddingsIndexRequest(
        std.heap.page_allocator,
        "sparse_idx",
        "body",
        .{
            .provider = .antfly,
            .model = "antfly-sparse-v1",
            .api_url = antfly_base_uri,
        },
    );
    defer std.heap.page_allocator.free(sparse_index_body);
    var sparse_index = try client.createTableIndex(api_base_uris[0], "docs", "sparse_idx", sparse_index_body);
    defer sparse_index.deinit(std.heap.page_allocator);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 96) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;

        var active_count: usize = 0;
        for (0..4) |i| {
            if (cluster.node(i).status(source_group_id) == .active) active_count += 1;
        }
        if (active_count == 3) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":615001,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        split_body,
    );

    var finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(615001)) |observation| {
            if (observation.status.phase == .finalized) {
                finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    var client_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group == 0 or right_group == 0 or left_group == right_group) {
            try cluster.stepAll();
            continue;
        }
        for (0..4) |i| {
            if (cluster.node(i).status(left_group) != .active or cluster.node(i).status(right_group) != .active) {
                client_index = i;
                break;
            }
        }
        if (client_index != null) break;
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    const routed_client_index = client_index orelse return error.TestExpectedEqual;
    const client_base = api_base_uris[routed_client_index];

    const left_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 96) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, left_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        if (found == null) {
            for (0..4) |i| {
                if (cluster.node(i).status(left_group) == .active) {
                    found = i;
                    break;
                }
            }
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    const right_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 96) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, right_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        if (found == null) {
            for (0..4) |i| {
                if (cluster.node(i).status(right_group) == .active) {
                    found = i;
                    break;
                }
            }
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    try ensureGroupEmbeddingIndexes(&cluster, roots[left_leader_index], left_group, "semantic_idx", "sparse_idx", 64);
    try ensureGroupEmbeddingIndexes(&cluster, roots[right_leader_index], right_group, "semantic_idx", "sparse_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[left_leader_index], left_group, "semantic_fixed_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[right_leader_index], right_group, "semantic_fixed_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[left_leader_index], left_group, "semantic_template_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[right_leader_index], right_group, "semantic_template_idx", 64);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"left alpha body","photo":"MEDIA_URL_A"},
        \\  "doc:z":{"title":"beta","body":"right beta body","photo":"MEDIA_URL_B"}
        \\}}
    );
    defer std.heap.page_allocator.free(batch_body);
    const media_url_doc_a = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/kitten.png", .{media_base_uri});
    defer std.heap.page_allocator.free(media_url_doc_a);
    const media_url_doc_b = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/doc.txt", .{media_base_uri});
    defer std.heap.page_allocator.free(media_url_doc_b);
    const batch_body_replaced = try std.mem.replaceOwned(u8, std.heap.page_allocator, batch_body, "MEDIA_URL_A", media_url_doc_a);
    defer std.heap.page_allocator.free(batch_body_replaced);
    const final_batch_body = try std.mem.replaceOwned(u8, std.heap.page_allocator, batch_body_replaced, "MEDIA_URL_B", media_url_doc_b);
    defer std.heap.page_allocator.free(final_batch_body);
    var batch = try client.fetchBatch(client_base, "docs", final_batch_body);
    defer batch.deinit(std.heap.page_allocator);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, batch.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_batch.value.inserted.?);

    const semantic_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_idx"}, 10);
    defer std.heap.page_allocator.free(semantic_query_body);
    var semantic_query = try client.fetchQuery(client_base, "docs", semantic_query_body);
    defer semantic_query.deinit(std.heap.page_allocator);
    var parsed_semantic = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, semantic_query.body, .{});
    defer parsed_semantic.deinit();
    const semantic_result = parsed_semantic.value.responses.?[0];
    try std.testing.expectEqualStrings("doc:a", semantic_result.hits.?.hits.?[0]._id);
    try expectQueryProfileSummary(std.heap.page_allocator, semantic_result.profile, 2, true);

    const fixed_chunked_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_fixed_idx"}, 10);
    defer std.heap.page_allocator.free(fixed_chunked_query_body);
    var fixed_chunked_query = try client.fetchQuery(client_base, "docs", fixed_chunked_query_body);
    defer fixed_chunked_query.deinit(std.heap.page_allocator);
    var parsed_fixed_chunked = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, fixed_chunked_query.body, .{});
    defer parsed_fixed_chunked.deinit();
    const fixed_chunked_result = parsed_fixed_chunked.value.responses.?[0];
    try std.testing.expectEqualStrings("doc:a", fixed_chunked_result.hits.?.hits.?[0]._id);
    try expectQueryProfileSummary(std.heap.page_allocator, fixed_chunked_result.profile, 2, true);

    const sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(std.heap.page_allocator, "sparse_idx", &.{ 7, 42 }, &.{ 1.5, 0.5 }, 10);
    defer std.heap.page_allocator.free(sparse_query_body);
    var sparse_query = try client.fetchQuery(client_base, "docs", sparse_query_body);
    defer sparse_query.deinit(std.heap.page_allocator);
    var parsed_sparse = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, sparse_query.body, .{});
    defer parsed_sparse.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_sparse.value.responses.?[0].hits.?.hits.?[0]._id);

    const template_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_template_idx"}, 10);
    defer std.heap.page_allocator.free(template_query_body);
    var template_query = try client.fetchQuery(client_base, "docs", template_query_body);
    defer template_query.deinit(std.heap.page_allocator);
    var parsed_template = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, template_query.body, .{});
    defer parsed_template.deinit();
    const template_result = parsed_template.value.responses.?[0];
    try std.testing.expectEqualStrings("doc:a", template_result.hits.?.hits.?[0]._id);
    try expectQueryProfileSummary(std.heap.page_allocator, template_result.profile, 2, true);
}

test "public api multi-node e2e routes semantic and sparse queries after merge from a non-host node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = Factory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = Factory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = Factory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = Factory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-multi-semantic-merge-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6160, root_a, cat_a),
        makeHostSimConfig(2, 6160, root_b, cat_b),
        makeHostSimConfig(3, 6160, root_c, cat_c),
        makeHostSimConfig(4, 6160, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try metadata_sim.MetadataHttpClusterSimulation.init(std.testing.allocator, 6160, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(4, &cluster, &metadata_admin_listeners, &metadata_admin_servers, &metadata_admin_sources, &metadata_apis);
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer for (metadata_apis) |uri| std.testing.allocator.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initInPlace(std.heap.page_allocator, .{});
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        &cluster,
        &roots,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        &api_base_uris,
    );
    defer for (&listeners) |*listener| listener.deinit();
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);

    var embed_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeEmbeddingProvider.executor());
    defer embed_listener.deinit();
    try embed_listener.start();
    const embed_base_uri = try embed_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(embed_base_uri);

    var antfly_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeAntflyProvider.executor());
    defer antfly_listener.deinit();
    try antfly_listener.start();
    const antfly_base_uri = try antfly_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(antfly_base_uri);

    var media_listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{}, FakeRemoteMedia.executor());
    defer media_listener.deinit();
    try media_listener.start();
    const media_base_uri = try media_listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(media_base_uri);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initInPlace(std.heap.page_allocator, .{});
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "api multi-node semantic merge docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    const semantic_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.heap.page_allocator,
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
    defer std.heap.page_allocator.free(semantic_index_body);
    var semantic_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_idx", semantic_index_body);
    defer semantic_index.deinit(std.heap.page_allocator);

    const fixed_chunked_index_body = try test_contract_helpers.encodeManagedEmbeddingsIndexRequest(
        std.heap.page_allocator,
        "semantic_fixed_idx",
        "body",
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
    defer std.heap.page_allocator.free(fixed_chunked_index_body);
    var fixed_chunked_index = try client.createTableIndex(api_base_uris[0], "docs", "semantic_fixed_idx", fixed_chunked_index_body);
    defer fixed_chunked_index.deinit(std.heap.page_allocator);

    const sparse_index_body = try test_contract_helpers.encodeManagedSparseEmbeddingsIndexRequest(
        std.heap.page_allocator,
        "sparse_idx",
        "body",
        .{
            .provider = .antfly,
            .model = "antfly-sparse-v1",
            .api_url = antfly_base_uri,
        },
    );
    defer std.heap.page_allocator.free(sparse_index_body);
    var sparse_index = try client.createTableIndex(api_base_uris[0], "docs", "sparse_idx", sparse_index_body);
    defer sparse_index.deinit(std.heap.page_allocator);

    var source_group_id: u64 = 0;
    var rounds: usize = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
        defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
        if (projected_ranges.len == 0) continue;
        source_group_id = projected_ranges[0].group_id;
        if (projected_ranges.len == 1) break;
    }
    try std.testing.expect(source_group_id != 0);

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":616001,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        split_body,
    );

    var split_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeSplitTransition(616001)) |observation| {
            if (observation.status.phase == .finalized) {
                split_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(split_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var left_group: u64 = 0;
    var right_group: u64 = 0;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:a")) orelse 0;
        right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:z")) orelse 0;
        if (left_group != 0 and right_group != 0 and left_group != right_group) break;
        try cluster.stepAll();
    }
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);

    const pre_merge_batch_body = try test_contract_helpers.normalizeBatchRequest(std.heap.page_allocator,
        \\{"inserts":{
        \\  "doc:a":{"title":"alpha","body":"left alpha body","photo":"MEDIA_URL_A"},
        \\  "doc:z":{"title":"beta","body":"right beta body","photo":"MEDIA_URL_B"}
        \\}}
    );
    defer std.heap.page_allocator.free(pre_merge_batch_body);
    const media_url_doc_a = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/kitten.png", .{media_base_uri});
    defer std.heap.page_allocator.free(media_url_doc_a);
    const media_url_doc_b = try std.fmt.allocPrint(std.heap.page_allocator, "{s}/doc.txt", .{media_base_uri});
    defer std.heap.page_allocator.free(media_url_doc_b);
    const pre_merge_batch_replaced = try std.mem.replaceOwned(u8, std.heap.page_allocator, pre_merge_batch_body, "MEDIA_URL_A", media_url_doc_a);
    defer std.heap.page_allocator.free(pre_merge_batch_replaced);
    const final_pre_merge_batch_body = try std.mem.replaceOwned(u8, std.heap.page_allocator, pre_merge_batch_replaced, "MEDIA_URL_B", media_url_doc_b);
    defer std.heap.page_allocator.free(final_pre_merge_batch_body);
    var pre_merge_batch = try client.fetchBatch(api_base_uris[0], "docs", final_pre_merge_batch_body);
    defer pre_merge_batch.deinit(std.heap.page_allocator);
    var parsed_pre_merge_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.heap.page_allocator, pre_merge_batch.body, .{});
    defer parsed_pre_merge_batch.deinit();
    try std.testing.expectEqual(@as(i64, 2), parsed_pre_merge_batch.value.inserted.?);

    const merge_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":616002,\"donor_group_id\":{d},\"receiver_group_id\":{d}}}", .{
        right_group,
        left_group,
    });
    defer std.testing.allocator.free(merge_body);
    try metadata_client.requestTableMerge(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        merge_body,
    );

    var merge_finalized = false;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        try cluster.stepAll();
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        if (try cluster.node(query_index).observeMergeTransition(616002)) |observation| {
            if (observation.receiver.phase == .finalized) {
                merge_finalized = true;
                break;
            }
        }
    }
    try std.testing.expect(merge_finalized);
    try metadata_client.triggerReallocate(metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index]);
    try cluster.stepAll();

    var client_index: ?usize = null;
    rounds = 0;
    while (rounds < 64) : (rounds += 1) {
        const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
        const merged_group = try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog_sources[query_index].iface(), "docs", "doc:z");
        if (merged_group == left_group) {
            for (0..4) |i| {
                if (cluster.node(i).status(left_group) != .active) {
                    client_index = i;
                    break;
                }
            }
            if (client_index != null) break;
        }
        try cluster.stepAll();
    }
    const client_base = api_base_uris[client_index orelse return error.TestExpectedEqual];

    const merged_leader_index = blk: {
        var found: ?usize = null;
        var leader_rounds: usize = 0;
        while (leader_rounds < 64) : (leader_rounds += 1) {
            found = currentGroupLeaderIndex(&cluster, left_group);
            if (found != null) break;
            try cluster.stepAll();
        }
        if (found == null) {
            for (0..4) |i| {
                if (cluster.node(i).status(left_group) == .active) {
                    found = i;
                    break;
                }
            }
        }
        break :blk found orelse return error.TestExpectedEqual;
    };
    try ensureGroupEmbeddingIndexes(&cluster, roots[merged_leader_index], left_group, "semantic_idx", "sparse_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[merged_leader_index], left_group, "semantic_fixed_idx", 64);
    try ensureGroupDenseIndex(&cluster, roots[merged_leader_index], left_group, "semantic_template_idx", 64);

    const semantic_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_idx"}, 10);
    defer std.heap.page_allocator.free(semantic_query_body);
    var semantic_query = try client.fetchQuery(client_base, "docs", semantic_query_body);
    defer semantic_query.deinit(std.heap.page_allocator);
    var parsed_semantic = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, semantic_query.body, .{});
    defer parsed_semantic.deinit();
    const semantic_result = parsed_semantic.value.responses.?[0];
    try std.testing.expectEqualStrings("doc:a", semantic_result.hits.?.hits.?[0]._id);
    try expectQueryProfileSummary(std.heap.page_allocator, semantic_result.profile, 1, false);

    const fixed_chunked_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_fixed_idx"}, 10);
    defer std.heap.page_allocator.free(fixed_chunked_query_body);
    var fixed_chunked_query = try client.fetchQuery(client_base, "docs", fixed_chunked_query_body);
    defer fixed_chunked_query.deinit(std.heap.page_allocator);
    var parsed_fixed_chunked = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, fixed_chunked_query.body, .{});
    defer parsed_fixed_chunked.deinit();
    const fixed_chunked_result = parsed_fixed_chunked.value.responses.?[0];
    try std.testing.expectEqualStrings("doc:a", fixed_chunked_result.hits.?.hits.?[0]._id);
    try expectQueryProfileSummary(std.heap.page_allocator, fixed_chunked_result.profile, 1, false);

    const sparse_query_body = try test_contract_helpers.encodeSparseEmbeddingsQueryRequest(std.heap.page_allocator, "sparse_idx", &.{ 7, 42 }, &.{ 1.5, 0.5 }, 10);
    defer std.heap.page_allocator.free(sparse_query_body);
    var sparse_query = try client.fetchQuery(client_base, "docs", sparse_query_body);
    defer sparse_query.deinit(std.heap.page_allocator);
    var parsed_sparse = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, sparse_query.body, .{});
    defer parsed_sparse.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed_sparse.value.responses.?[0].hits.?.hits.?[0]._id);

    const template_query_body = try test_contract_helpers.encodeSemanticQueryRequest(std.heap.page_allocator, "alpha concept", &.{"semantic_template_idx"}, 10);
    defer std.heap.page_allocator.free(template_query_body);
    var template_query = try client.fetchQuery(client_base, "docs", template_query_body);
    defer template_query.deinit(std.heap.page_allocator);
    var parsed_template = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, template_query.body, .{});
    defer parsed_template.deinit();
    const template_result = parsed_template.value.responses.?[0];
    try std.testing.expectEqualStrings("doc:a", template_result.hits.?.hits.?[0]._id);
    try expectQueryProfileSummary(std.heap.page_allocator, template_result.profile, 1, false);
}
