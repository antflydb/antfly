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
const scraping = @import("antfly_scraping");
const fs_paths = @import("../common/fs_paths.zig");
const common_secrets = @import("../common/secrets.zig");
const search_pattern_filter = @import("../search/pattern_filter.zig");
const backups_api = @import("backups.zig");
const batch_api = @import("batch.zig");
const cluster_api_http = @import("cluster_api_http.zig");
const public_table_http = @import("public_table_http.zig");
const linear_merge_api = @import("linear_merge.zig");
const cluster = @import("cluster.zig");
const indexes_api = @import("indexes.zig");
const table_contract = @import("table_contract.zig");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_reconciler = @import("../metadata/reconciler.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const metadata_table_workflow = @import("../metadata/table_workflow.zig");
const schema_mod = @import("../schema/mod.zig");
const http_common = @import("../raft/transport/http_common.zig");
const raft_host = @import("../raft/host.zig");
const raft_mod = @import("../raft/mod.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const table_catalog = @import("table_catalog.zig");
const tables_api = @import("tables.zig");
const table_reads = @import("table_reads.zig");
const table_router = @import("table_router.zig");
const table_writes = @import("table_writes.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const public_search_request = @import("public_search_request.zig");
const query_builder_agent = @import("query_builder_agent.zig");
const retrieval_agent = @import("retrieval_agent.zig");
const distributed_graph = @import("distributed_graph.zig");
const distributed_join = @import("distributed_join.zig");
const distributed_txn = @import("distributed_txn.zig");
const http_internal_routes = @import("http_internal_routes.zig");
const http_internal_group_read_routes = @import("http_internal_group_read_routes.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const transactions_api = @import("transactions.zig");
const docstore_mod = @import("../storage/docstore.zig");
const routes = @import("http_routes.zig");
const runtime_status = @import("runtime_status.zig");
const test_contract_helpers = @import("test_contract_helpers.zig");
const platform_time = @import("../platform/time.zig");
const foreign_mod = @import("../foreign/mod.zig");
const foreign_sources_api = @import("foreign_sources.zig");
const json_helpers = @import("json_helpers.zig");
const eval_openapi = @import("antfly_eval_openapi");
const schema_openapi = @import("antfly_schema_openapi");
const metadata_service = @import("../metadata/service.zig");
const metadata_server = @import("../metadata/server.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const generating_runtime = @import("../generating/mod.zig");
const usermgr = @import("../usermgr/mod.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const usermgr_openapi = @import("antfly_usermgr_openapi");
const casbin = @import("antfly_casbin");
const httpx = @import("httpx");
const mcp = @import("antfly_mcp");
const a2a = @import("antfly_a2a");
const protocol_adapters = @import("protocol_adapters.zig");
const parseJsonValueAlloc = json_helpers.parseJsonValueAlloc;
const parseOwnedJsonValueAlloc = json_helpers.parseOwnedJsonValueAlloc;
const parseOwnedJsonObjectMapAlloc = json_helpers.parseOwnedJsonObjectMapAlloc;

const TestSseEvent = struct {
    event: []const u8,
    data: []const u8,
};

const QueryBuilderIndexContext = struct {
    full_text_index_metadata: []const query_builder_agent.QueryBuilderFullTextIndex = &.{},
    embedding_index_metadata: []const query_builder_agent.QueryBuilderEmbeddingIndex = &.{},
    graph_index_metadata: []const query_builder_agent.QueryBuilderGraphIndex = &.{},
};

pub const QueryBuilderRuntimeQueryRequestValidatorContext = struct {
    server: *ApiHttpServer,
    source: table_reads.TableReadSource,
    table_name: []const u8,

    pub fn iface(self: *@This()) query_builder_agent.QueryBuilderRuntimeQueryRequestValidator {
        return .{
            .ptr = self,
            .vtable = &.{
                .validate_query_request = validateQueryRequest,
                .preflight_query_request = preflightQueryRequest,
            },
        };
    }

    fn validateQueryRequest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        query_request: metadata_openapi.QueryRequest,
    ) !?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var semantic_resolver = SemanticStatusResolver{
            .source = self.server.source,
            .local_termite_provider = self.server.local_termite_provider,
            .remote_content = self.server.cfg.remote_content,
        };
        const encoded = try std.json.Stringify.valueAlloc(alloc, query_request, .{});
        defer alloc.free(encoded);
        var parsed = query_api.parsePublicQueryRequest(alloc, semantic_resolver.iface(), self.table_name, encoded) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try std.fmt.allocPrint(alloc, "query_request failed runtime parse: {s}", .{@errorName(err)}),
            else => return err,
        };
        defer parsed.deinit(alloc);

        var summary = (try self.source.preflightQuery(alloc, self.table_name, parsed.req, .read_index, 0)) orelse return null;
        summary.deinit(alloc);
        return null;
    }

    fn preflightQueryRequest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        query_request: metadata_openapi.QueryRequest,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try runtimePreflightQueryRequest(self, alloc, query_request, max_work);
    }

    fn runtimePreflightQueryRequest(
        self: *@This(),
        alloc: std.mem.Allocator,
        query_request: metadata_openapi.QueryRequest,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        var semantic_resolver = SemanticStatusResolver{
            .source = self.server.source,
            .local_termite_provider = self.server.local_termite_provider,
            .remote_content = self.server.cfg.remote_content,
        };
        const encoded = try std.json.Stringify.valueAlloc(alloc, query_request, .{});
        defer alloc.free(encoded);
        var parsed = query_api.parsePublicQueryRequest(alloc, semantic_resolver.iface(), self.table_name, encoded) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return null,
            else => return err,
        };
        defer parsed.deinit(alloc);

        return self.source.preflightQuery(alloc, self.table_name, parsed.req, .read_index, max_work) catch |err| switch (err) {
            error.InvalidArgument, error.IndexNotFound => null,
            else => return err,
        };
    }
};

const QueryBuilderIndexContextType = enum {
    full_text,
    embeddings,
    graph,
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

pub const ApiHttpServerConfig = struct {
    auth_enabled: bool = false,
    swarm_mode: bool = false,
    backend_runtime: ?*db_mod.background_runtime.BackendRuntime = null,
    foreign_registry: ?*const foreign_mod.Registry = null,
    shard_ops: ?raft_mod.ShardOperationAdapter = null,
    shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    secret_store: ?*common_secrets.FileStore = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    user_manager: ?*usermgr.UserManager = null,
    session_router: ?table_router.HostedGroupRouter = null,
    session_executor: ?http_common.RequestExecutor = null,
    session_store: ?*transactions_api.DurableSessionStore = null,
    session_store_path: ?[]const u8 = null,
    join_job_store_path: ?[]const u8 = null,
    join_job_lease_ttl_ms: ?u64 = null,
    join_job_retention_ms: ?u64 = null,
    session_ttl_ns: ?u64 = null,
    session_cleanup_interval_ns: ?u64 = null,
    session_owner_lease_ttl_ns: ?u64 = null,
    session_owner_lease_renew_interval_ns: ?u64 = null,
    session_savepoint_limit: ?usize = null,
};

pub const SemanticStatusResolver = struct {
    source: StatusSource,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,

    pub fn iface(self: *SemanticStatusResolver) query_contract.SemanticResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve_dense_query = resolveDenseQuery,
            },
        };
    }

    fn resolveDenseQuery(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        semantic_search: []const u8,
        embedding_template: ?[]const u8,
        limit: u32,
    ) !db_mod.types.DenseKnnQuery {
        const self: *SemanticStatusResolver = @ptrCast(@alignCast(ptr));
        return try http_internal_group_read_routes.planSemanticQuery(.{
            .ptr = self.source.ptr,
            .admin_snapshot = self.source.vtable.admin_snapshot orelse return error.UnsupportedQueryRequest,
            .free_admin_snapshot = self.source.vtable.free_admin_snapshot orelse return error.UnsupportedQueryRequest,
            .local_termite_provider = self.local_termite_provider,
            .remote_content = self.remote_content,
        }, alloc, table_name, index_name, semantic_search, embedding_template, limit);
    }
};

pub const TableVisibility = enum {
    present,
    absent,
};

pub const AuthenticatedIdentity = struct {
    username: []u8,
    permissions: []usermgr.Permission = &.{},
    row_filter: []usermgr.RowFilterEntry = &.{},
    metadata_json: []u8 = &.{},
    roles: [][]u8 = &.{},

    pub fn deinit(self: *AuthenticatedIdentity, alloc: std.mem.Allocator) void {
        alloc.free(self.username);
        for (self.permissions) |*permission| permission.deinit(alloc);
        if (self.permissions.len > 0) alloc.free(self.permissions);
        for (self.row_filter) |*entry| entry.deinit(alloc);
        if (self.row_filter.len > 0) alloc.free(self.row_filter);
        if (self.metadata_json.len > 0) alloc.free(self.metadata_json);
        for (self.roles) |role| alloc.free(role);
        if (self.roles.len > 0) alloc.free(self.roles);
        self.* = undefined;
    }
};

pub const StatusSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        status: *const fn (ptr: *anyopaque) anyerror!metadata_api.MetadataStatus,
        admin_snapshot: ?*const fn (ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot = null,
        cached_admin_snapshot: ?*const fn (ptr: *anyopaque) anyerror!?metadata_api.AdminSnapshot = null,
        free_admin_snapshot: ?*const fn (ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void = null,
        create_table: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) anyerror!void = null,
        restore_table: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) anyerror!void = null,
        drop_table: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) anyerror!void = null,
        update_schema: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) anyerror!void = null,
        create_index: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) anyerror!void = null,
        drop_index: ?*const fn (ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) anyerror!void = null,
        wait_table_lifecycle: ?*const fn (ptr: *anyopaque, table_name: []const u8, expected: TableVisibility) anyerror!void = null,
        wait_table_projection: ?*const fn (ptr: *anyopaque, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) anyerror!void = null,
        run_round: ?*const fn (ptr: *anyopaque) anyerror!void = null,
        get_join_shuffle_lease: ?*const fn (ptr: *anyopaque, job_id: u64) anyerror!?metadata_table_manager.ShuffleJoinLeaseRecord = null,
        upsert_join_shuffle_lease: ?*const fn (ptr: *anyopaque, record: metadata_table_manager.ShuffleJoinLeaseRecord) anyerror!void = null,
        remove_join_shuffle_lease: ?*const fn (ptr: *anyopaque, job_id: u64) anyerror!void = null,
    };

    pub fn status(self: StatusSource) !metadata_api.MetadataStatus {
        return try self.vtable.status(self.ptr);
    }

    pub fn adminSnapshot(self: StatusSource) !?metadata_api.AdminSnapshot {
        const fn_ptr = self.vtable.admin_snapshot orelse return null;
        return try fn_ptr(self.ptr);
    }

    pub fn cachedAdminSnapshot(self: StatusSource) !?metadata_api.AdminSnapshot {
        const fn_ptr = self.vtable.cached_admin_snapshot orelse return null;
        return try fn_ptr(self.ptr);
    }

    pub fn freeAdminSnapshot(self: StatusSource, snapshot: *metadata_api.AdminSnapshot) void {
        const fn_ptr = self.vtable.free_admin_snapshot orelse return;
        fn_ptr(self.ptr, snapshot);
    }

    pub fn createTable(self: StatusSource, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) !void {
        const fn_ptr = self.vtable.create_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, req);
    }

    pub fn restoreTable(self: StatusSource, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !bool {
        const fn_ptr = self.vtable.restore_table orelse return false;
        try fn_ptr(self.ptr, alloc, table_name, location_uri, backup_id);
        return true;
    }

    pub fn dropTable(self: StatusSource, alloc: std.mem.Allocator, table_name: []const u8) !void {
        const fn_ptr = self.vtable.drop_table orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn updateSchema(self: StatusSource, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const fn_ptr = self.vtable.update_schema orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, schema_json);
    }

    pub fn createIndex(self: StatusSource, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const fn_ptr = self.vtable.create_index orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, index_name, index_json);
    }

    pub fn dropIndex(self: StatusSource, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const fn_ptr = self.vtable.drop_index orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, index_name);
    }

    pub fn waitTableLifecycle(self: StatusSource, table_name: []const u8, expected: TableVisibility) !bool {
        const fn_ptr = self.vtable.wait_table_lifecycle orelse return false;
        try fn_ptr(self.ptr, table_name, expected);
        return true;
    }

    pub fn waitTableProjection(self: StatusSource, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) !bool {
        const fn_ptr = self.vtable.wait_table_projection orelse return false;
        try fn_ptr(self.ptr, table_name, schema_json, indexes_json);
        return true;
    }

    pub fn runRound(self: StatusSource) !bool {
        const fn_ptr = self.vtable.run_round orelse return false;
        try fn_ptr(self.ptr);
        return true;
    }

    pub fn getJoinShuffleLease(self: StatusSource, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
        const fn_ptr = self.vtable.get_join_shuffle_lease orelse return null;
        return try fn_ptr(self.ptr, job_id);
    }

    pub fn upsertJoinShuffleLease(self: StatusSource, record: metadata_table_manager.ShuffleJoinLeaseRecord) !bool {
        const fn_ptr = self.vtable.upsert_join_shuffle_lease orelse return false;
        try fn_ptr(self.ptr, record);
        return true;
    }

    pub fn removeJoinShuffleLease(self: StatusSource, job_id: u64) !bool {
        const fn_ptr = self.vtable.remove_join_shuffle_lease orelse return false;
        try fn_ptr(self.ptr, job_id);
        return true;
    }

    fn makeServiceVTable(comptime T: type) VTable {
        const Gen = struct {
            fn cast(ptr: *anyopaque) *T {
                return @ptrCast(@alignCast(ptr));
            }

            fn status(ptr: *anyopaque) anyerror!metadata_api.MetadataStatus {
                return try cast(ptr).status();
            }

            fn adminSnapshot(ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot {
                return try cast(ptr).adminSnapshot();
            }

            fn cachedAdminSnapshot(ptr: *anyopaque) anyerror!?metadata_api.AdminSnapshot {
                return try cast(ptr).adminSnapshot();
            }

            fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
                cast(ptr).freeAdminSnapshot(snapshot);
            }

            fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) anyerror!void {
                return try createTableOnService(cast(ptr), alloc, table_name, req);
            }

            fn restoreTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) anyerror!void {
                return try persistRestoreTableIntent(cast(ptr), alloc, table_name, location_uri, backup_id, serviceSecretStore(cast(ptr)));
            }

            fn dropTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) anyerror!void {
                return try dropTableOnService(cast(ptr), alloc, table_name);
            }

            fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) anyerror!void {
                return try updateSchemaOnService(cast(ptr), alloc, table_name, schema_json);
            }

            fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) anyerror!void {
                return try createIndexOnService(cast(ptr), alloc, table_name, index_name, index_json);
            }

            fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) anyerror!void {
                return try dropIndexOnService(cast(ptr), alloc, table_name, index_name);
            }

            fn waitTableLifecycle(ptr: *anyopaque, table_name: []const u8, expected: TableVisibility) anyerror!void {
                return try cast(ptr).waitForTableLifecycle(table_name, switch (expected) {
                    .present => .present,
                    .absent => .absent,
                });
            }

            fn waitTableProjection(ptr: *anyopaque, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) anyerror!void {
                return try cast(ptr).waitForTableProjection(table_name, .{
                    .schema_json = schema_json,
                    .indexes_json = indexes_json,
                });
            }

            fn runRound(ptr: *anyopaque) anyerror!void {
                return try cast(ptr).runRound();
            }

            fn getJoinShuffleLease(ptr: *anyopaque, job_id: u64) anyerror!?metadata_table_manager.ShuffleJoinLeaseRecord {
                return try cast(ptr).getProjectedShuffleJoinLease(job_id);
            }

            fn upsertJoinShuffleLease(ptr: *anyopaque, record: metadata_table_manager.ShuffleJoinLeaseRecord) anyerror!void {
                try cast(ptr).upsertShuffleJoinLease(record);
            }

            fn removeJoinShuffleLease(ptr: *anyopaque, job_id: u64) anyerror!void {
                try cast(ptr).removeShuffleJoinLease(job_id);
            }
        };

        return .{
            .status = Gen.status,
            .admin_snapshot = Gen.adminSnapshot,
            .cached_admin_snapshot = Gen.cachedAdminSnapshot,
            .free_admin_snapshot = Gen.freeAdminSnapshot,
            .create_table = Gen.createTable,
            .restore_table = Gen.restoreTable,
            .drop_table = Gen.dropTable,
            .update_schema = Gen.updateSchema,
            .create_index = Gen.createIndex,
            .drop_index = Gen.dropIndex,
            .wait_table_lifecycle = Gen.waitTableLifecycle,
            .wait_table_projection = Gen.waitTableProjection,
            .run_round = Gen.runRound,
            .get_join_shuffle_lease = Gen.getJoinShuffleLease,
            .upsert_join_shuffle_lease = Gen.upsertJoinShuffleLease,
            .remove_join_shuffle_lease = Gen.removeJoinShuffleLease,
        };
    }

    pub fn fromMetadataHttpService(svc: *metadata_service.MetadataHttpService) StatusSource {
        return .{ .ptr = svc, .vtable = &comptime makeServiceVTable(metadata_service.MetadataHttpService) };
    }

    pub fn fromMetadataService(svc: *metadata_service.MetadataService) StatusSource {
        return .{ .ptr = svc, .vtable = &comptime makeServiceVTable(metadata_service.MetadataService) };
    }

    pub fn fromMetadataServer(srv: *metadata_server.MetadataServer) StatusSource {
        return fromMetadataHttpService(srv.svc);
    }
};

const RestoreMetadataSpec = struct {
    manifest: backups_api.TableBackupManifest,
    table: metadata_table_manager.TableRecord,
    ranges: []metadata_table_manager.RangeRecord,

    fn deinit(self: *RestoreMetadataSpec, alloc: std.mem.Allocator) void {
        self.manifest.deinit(alloc);
        metadata_table_manager.freeTable(alloc, self.table);
        for (self.ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(self.ranges);
        self.* = undefined;
    }
};

fn loadRestoreMetadataSpec(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    location_uri: []const u8,
    backup_id: []const u8,
    secret_store: ?*common_secrets.FileStore,
) !RestoreMetadataSpec {
    var location = try backups_api.openBackupLocationWithSecrets(alloc, location_uri, secret_store);
    defer location.deinit(alloc);
    var manifest = backups_api.readManifestFromLocation(alloc, &location, backup_id) catch return error.InvalidBackupRequest;
    errdefer manifest.deinit(alloc);
    if (!std.mem.eql(u8, manifest.table_name, table_name)) return error.InvalidBackupRequest;
    const table = backups_api.deriveRestoreTableRecord(alloc, table_name, location_uri, &manifest) catch |err| switch (err) {
        error.UnsupportedBackupMigrationState => return error.UnsupportedBackupMigrationState,
        else => return err,
    };
    errdefer metadata_table_manager.freeTable(alloc, table);
    const ranges = try backups_api.deriveRestoreRanges(alloc, table.table_id, location_uri, &manifest);
    errdefer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    return .{
        .manifest = manifest,
        .table = table,
        .ranges = ranges,
    };
}

fn createTableOnService(svc: anytype, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) !void {
    var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
    defer workflow.deinit();
    var normalized_req = req;
    var expanded_indexes_json: ?[]u8 = null;
    defer if (expanded_indexes_json) |value| alloc.free(value);
    const indexes_json = req.indexes_json orelse tables_api.default_indexes_json;
    expanded_indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(alloc, table_name, indexes_json, tables_api.effectiveSchemaJson(req.schema_json));
    normalized_req.indexes_json = expanded_indexes_json;
    const table = tables_api.deriveTableRecord(table_name, normalized_req);
    const ranges = try tables_api.deriveInitialRanges(alloc, table);
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }
    _ = try workflow.createTableWithRanges(svc, table, ranges);
    try svc.runRound();
}

fn dropTableOnService(svc: anytype, alloc: std.mem.Allocator, table_name: []const u8) !void {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
    defer workflow.deinit();
    _ = try workflow.dropTable(svc, table.table_id);
    try svc.runRound();
}

fn updateSchemaOnService(svc: anytype, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    const updated = try tables_api.applySchemaUpdateRecord(alloc, table, schema_json);
    defer metadata_table_manager.freeTable(alloc, updated);
    try svc.upsertTable(updated);
    try svc.runRound();
}

fn createIndexOnService(svc: anytype, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    const expanded_index_json = try tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table_name, index_json, table.schema_json);
    defer alloc.free(expanded_index_json);

    var updated_record = table.*;
    updated_record.indexes_json = try indexes_api.addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, expanded_index_json);
    defer alloc.free(updated_record.indexes_json);
    try svc.upsertTable(updated_record);
    try svc.runRound();
}

fn dropIndexOnService(svc: anytype, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    const indexes_json = (try indexes_api.removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
    defer alloc.free(indexes_json);
    var updated_record = table.*;
    updated_record.indexes_json = indexes_json;
    try svc.upsertTable(updated_record);
    try svc.runRound();
}

fn serviceSecretStore(service: anytype) ?*common_secrets.FileStore {
    const Ptr = @TypeOf(service);
    const Service = std.meta.Child(Ptr);
    if (comptime @hasField(Service, "secret_store")) {
        return service.secret_store;
    }
    return null;
}

fn persistRestoreTableIntent(service: anytype, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8, secret_store: ?*common_secrets.FileStore) !void {
    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    if (tables_api.findTableByName(&snapshot, table_name) != null) return error.TableAlreadyExists;

    var spec = try loadRestoreMetadataSpec(alloc, table_name, location_uri, backup_id, secret_store);
    defer spec.deinit(alloc);

    var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
    defer workflow.deinit();
    _ = try workflow.createTableWithRanges(service, spec.table, spec.ranges);
}

pub const ApiHttpServer = struct {
    const SupportedJoinRequest = distributed_join.SupportedJoinRequest;
    const SupportedJoinFilters = distributed_join.SupportedJoinFilters;
    const JoinShuffleJobPhase = distributed_join.JoinShuffleJobPhase;
    const JoinShuffleExecutionMode = distributed_join.JoinShuffleExecutionMode;
    const JoinPartitionExecutionResult = distributed_join.JoinPartitionExecutionResult;
    const RightJoinQueryResult = distributed_join.RightJoinQueryResult;
    const PlannedJoinExecution = distributed_join.PlannedJoinExecution;
    const JoinedQueryStats = distributed_join.JoinedQueryStats;
    const ParsedSupportedJoinRequest = distributed_join.ParsedSupportedJoinRequest;
    const JoinShuffleResumeState = distributed_join.JoinShuffleResumeState;
    const phaseString = distributed_join.phaseString;
    const phaseFromString = distributed_join.phaseFromString;
    const joinJobNowMillis = distributed_join.joinJobNowMillis;
    const preferredFinalizerStartIndex = distributed_join.preferredFinalizerStartIndex;
    const JoinShuffleJobState = distributed_join.JoinShuffleJobState;
    const extractJsonPathValue = distributed_join.extractJsonPathValue;
    const parseSupportedJoinRequest = distributed_join.parseSupportedJoinRequest;
    fn encodeJoinPartitionRequest(
        _: *const ApiHttpServer,
        alloc: std.mem.Allocator,
        job_id: ?u64,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        appended_left_field: bool,
        partition_index: usize,
        partition_count: usize,
        right_group_ids: []const u64,
    ) ![]u8 {
        return distributed_join.encodeJoinPartitionRequest(alloc, job_id, join, left_hits, appended_left_field, partition_index, partition_count, right_group_ids);
    }

    fn encodeJoinFinalizeRequest(
        _: *const ApiHttpServer,
        alloc: std.mem.Allocator,
        job_id: u64,
        handoff_owner_group_id: ?u64,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        left_fields: []const std.json.Value,
        appended_left_field: bool,
        shuffle_partitions: usize,
    ) ![]u8 {
        return distributed_join.encodeJoinFinalizeRequest(alloc, job_id, handoff_owner_group_id, join, left_hits, left_fields, appended_left_field, shuffle_partitions);
    }
    fn encodeJoinJobState(_: *const ApiHttpServer, alloc: std.mem.Allocator, job_id: u64, state: JoinShuffleJobState) ![]u8 {
        return distributed_join.encodeJoinJobState(alloc, job_id, state);
    }

    fn executeJoinPartitionWorkerLocal(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        worker_group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !distributed_join.JoinPartitionExecutionResult {
        self.join_job_store.setContext(self.joinContext());
        return distributed_join.executeJoinPartitionWorkerLocal(self.joinContext(), &self.join_job_store, alloc, source, worker_group_id, table_name, body);
    }

    fn executeJoinFinalizeWorkerLocal(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        finalizer_group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !distributed_join.JoinPartitionExecutionResult {
        self.join_job_store.setContext(self.joinContext());
        return distributed_join.executeJoinFinalizeWorkerLocal(self.joinContext(), &self.join_job_store, alloc, source, finalizer_group_id, table_name, body);
    }

    fn planSupportedJoinExecution(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) !PlannedJoinExecution {
        return distributed_join.planSupportedJoinExecution(self.joinContext(), alloc, table_name, join, left_hits, foreign_sources);
    }

    fn encodeJoinPartitionResponse(self: *const ApiHttpServer, alloc: std.mem.Allocator, result: distributed_join.JoinPartitionExecutionResult) ![]u8 {
        return self.join_job_store.encodeJoinPartitionResponse(alloc, result);
    }

    pub fn executeSupportedRightJoinQuery(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        plan: PlannedJoinExecution,
    ) !RightJoinQueryResult {
        self.join_job_store.setContext(self.joinContext());
        return distributed_join.executeSupportedRightJoinQuery(self.joinContext(), &self.join_job_store, alloc, source, join, left_hits, plan, .{});
    }

    pub fn executeSupportedDistributedJoinFinalized(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        left_fields: []const std.json.Value,
        appended_left_field: bool,
        plan: PlannedJoinExecution,
    ) !?JoinPartitionExecutionResult {
        self.join_job_store.setContext(self.joinContext());
        return distributed_join.executeSupportedDistributedJoinFinalized(self.joinContext(), &self.join_job_store, alloc, source, join, left_hits, left_fields, appended_left_field, plan);
    }

    pub fn executeSupportedDistributedJoinPartitions(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        job_id: ?u64,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        appended_left_field: bool,
        plan: PlannedJoinExecution,
        resume_state: ?distributed_join.JoinShuffleResumeState,
    ) !?JoinPartitionExecutionResult {
        self.join_job_store.setContext(self.joinContext());
        return distributed_join.executeSupportedDistributedJoinPartitions(self.joinContext(), &self.join_job_store, alloc, source, job_id, join, left_hits, appended_left_field, plan, resume_state);
    }

    pub fn recordJoinJobStart(self: *ApiHttpServer, job_id: u64, owner_group_id: ?u64, total_partitions: usize) !void {
        self.join_job_store.setContext(self.joinContext());
        return self.join_job_store.recordJoinJobStart(job_id, owner_group_id, total_partitions);
    }

    pub fn recordJoinJobProgress(self: *ApiHttpServer, job_id: u64, next_partition_index: usize, partial_result: distributed_join.JoinPartitionExecutionResult) !void {
        self.join_job_store.setContext(self.joinContext());
        return self.join_job_store.recordJoinJobProgress(job_id, next_partition_index, partial_result);
    }

    pub fn extractJoinValueFromHit(hit: std.json.Value, field_name: []const u8) ?std.json.Value {
        return distributed_join.extractJoinValueFromHit(hit, field_name);
    }

    pub fn stableDistributedJoinJobId(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        appended_left_field: bool,
        shuffle_partitions: usize,
    ) !u64 {
        return self.join_job_store.stableDistributedJoinJobId(alloc, join, left_hits, appended_left_field, shuffle_partitions);
    }

    pub fn executeForeignRightJoinQuery(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        foreign_source: foreign_mod.PostgresConfig,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) !RightJoinQueryResult {
        return distributed_join.executeForeignRightJoinQuery(self.joinContext(), &self.join_job_store, alloc, source, foreign_source, join, left_hits, foreign_sources);
    }

    pub fn partitionForJoinValue(value: std.json.Value, partition_count: usize) usize {
        return distributed_join.partitionForJoinValue(value, partition_count);
    }

    alloc: std.mem.Allocator,
    cfg: ApiHttpServerConfig,
    source: StatusSource,
    table_reads: ?table_reads.TableReadSource = null,
    table_writes: ?table_writes.TableWriteSource = null,
    local_termite_provider: ?managed_embedder.LocalTermiteProvider = null,
    foreign_registry: ?*const foreign_mod.Registry = null,
    owned_foreign_registry: ?*foreign_mod.Registry = null,
    txn_sessions: transactions_api.SessionRegistry = .{},
    last_session_cleanup_ns: u64 = 0,
    last_session_lease_renew_ns: u64 = 0,
    created_at_ns: u64 = 0,
    request_count: std.atomic.Value(u64) = .init(0),
    first_request_started_at_ns: std.atomic.Value(u64) = .init(0),
    opened_session_store: ?*transactions_api.OpenedSessionStore = null,
    join_job_store: distributed_join.JoinJobStore = .{ .alloc = undefined, .cfg = .{} },
    mcp_sessions: mcp.InMemorySessionStore = .{},
    a2a_tasks: a2a.InMemoryTaskStore = .{},

    pub const RequestStats = struct {
        request_count: u64 = 0,
        first_request_started_at_ns: u64 = 0,
        first_request_elapsed_ms: u64 = 0,
    };

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: ApiHttpServerConfig,
        source: StatusSource,
        table_read_source: ?table_reads.TableReadSource,
        table_write_source: ?table_writes.TableWriteSource,
    ) ApiHttpServer {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .source = source,
            .table_reads = table_read_source,
            .table_writes = table_write_source,
            .foreign_registry = cfg.foreign_registry,
            .created_at_ns = platform_time.monotonicNs(),
            .txn_sessions = transactions_api.SessionRegistry.initWithOptions(
                cfg.session_store,
                if (cfg.session_store != null and cfg.session_owner_lease_ttl_ns != null)
                    transactions_api.SessionLeaseStore.init(alloc, cfg.session_store.?.store)
                else
                    null,
                cfg.session_owner_lease_ttl_ns,
                cfg.session_savepoint_limit,
            ),
            .join_job_store = distributed_join.JoinJobStore.init(alloc, .{
                .join_job_store_path = cfg.join_job_store_path,
                .join_job_lease_ttl_ms = cfg.join_job_lease_ttl_ms,
                .join_job_retention_ms = cfg.join_job_retention_ms,
            }),
            .mcp_sessions = mcp.InMemorySessionStore.init(alloc),
            .a2a_tasks = a2a.InMemoryTaskStore.init(alloc),
        };
    }

    pub fn requestStats(self: *const ApiHttpServer) RequestStats {
        const request_count = self.request_count.load(.monotonic);
        const first_request_started_at_ns = self.first_request_started_at_ns.load(.monotonic);
        return .{
            .request_count = request_count,
            .first_request_started_at_ns = first_request_started_at_ns,
            .first_request_elapsed_ms = if (first_request_started_at_ns == 0 or first_request_started_at_ns < self.created_at_ns)
                0
            else
                @intCast(@divTrunc(first_request_started_at_ns - self.created_at_ns, std.time.ns_per_ms)),
        };
    }

    pub fn initWithConfig(
        alloc: std.mem.Allocator,
        cfg: ApiHttpServerConfig,
        source: StatusSource,
        table_read_source: ?table_reads.TableReadSource,
        table_write_source: ?table_writes.TableWriteSource,
    ) !ApiHttpServer {
        var effective_cfg = cfg;
        var server = ApiHttpServer.init(alloc, effective_cfg, source, table_read_source, table_write_source);
        if (cfg.session_store_path) |path| {
            if (cfg.session_store != null) return error.InvalidApiServerConfig;
            const opened = try alloc.create(transactions_api.OpenedSessionStore);
            errdefer alloc.destroy(opened);
            opened.* = try transactions_api.OpenedSessionStore.open(alloc, path);
            server.opened_session_store = opened;
            effective_cfg.session_store = opened.durableStore();
            server.cfg = effective_cfg;
            server.txn_sessions = transactions_api.SessionRegistry.initWithOptions(
                effective_cfg.session_store,
                opened.leaseStore().*,
                effective_cfg.session_owner_lease_ttl_ns,
                effective_cfg.session_savepoint_limit,
            );
        }
        if (cfg.join_job_store_path orelse cfg.session_store_path) |base_path| {
            const join_job_path = if (cfg.join_job_store_path != null)
                try alloc.dupe(u8, base_path)
            else
                try std.fmt.allocPrint(alloc, "{s}.join_jobs", .{base_path});
            defer alloc.free(join_job_path);
            const opened = try alloc.create(distributed_join.OpenedJoinJobStore);
            errdefer alloc.destroy(opened);
            opened.* = try distributed_join.OpenedJoinJobStore.open(alloc, join_job_path);
            server.join_job_store.opened_join_job_store = opened;
        }
        return server;
    }

    pub fn setForeignRegistry(self: *ApiHttpServer, registry: *const foreign_mod.Registry) void {
        self.foreign_registry = registry;
        self.cfg.foreign_registry = registry;
    }

    fn ensureForeignRegistry(self: *ApiHttpServer) !*const foreign_mod.Registry {
        if (self.foreign_registry) |registry| return registry;
        const registry = try self.alloc.create(foreign_mod.Registry);
        errdefer self.alloc.destroy(registry);
        registry.* = .{};
        errdefer registry.deinit(self.alloc);
        try foreign_mod.registerDefaultPostgresExecutor(self.alloc, registry);
        self.owned_foreign_registry = registry;
        self.foreign_registry = registry;
        self.cfg.foreign_registry = registry;
        return registry;
    }

    pub fn deinit(self: *ApiHttpServer) void {
        self.mcp_sessions.deinit(self.alloc);
        self.a2a_tasks.deinit(self.alloc);
        self.txn_sessions.deinit(self.alloc);
        if (self.opened_session_store) |opened| {
            opened.deinit();
            self.alloc.destroy(opened);
        }
        self.join_job_store.deinit();
        if (self.owned_foreign_registry) |registry| {
            registry.deinit(self.alloc);
            self.alloc.destroy(registry);
        }
        self.* = undefined;
    }

    pub fn joinContext(self: *ApiHttpServer) distributed_join.JoinContext {
        return .{
            .ptr = self,
            .vtable = &join_context_vtable,
        };
    }

    const join_context_vtable = distributed_join.JoinContext.VTable{
        .admin_snapshot = joinCtxAdminSnapshot,
        .free_admin_snapshot = joinCtxFreeAdminSnapshot,
        .get_join_shuffle_lease = joinCtxGetJoinShuffleLease,
        .upsert_join_shuffle_lease = joinCtxUpsertJoinShuffleLease,
        .remove_join_shuffle_lease = joinCtxRemoveJoinShuffleLease,
        .execute_plain_query = joinCtxExecutePlainQuery,
        .execute_query_dispatch = joinCtxExecuteQueryDispatch,
        .build_owned_search_request = joinCtxBuildOwnedSearchRequest,
        .ensure_foreign_registry = joinCtxEnsureForeignRegistry,
    };

    fn joinCtxAdminSnapshot(ptr: *anyopaque) anyerror!?metadata_api.AdminSnapshot {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.source.adminSnapshot();
    }

    fn joinCtxFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        self.source.freeAdminSnapshot(snapshot);
    }

    fn joinCtxGetJoinShuffleLease(ptr: *anyopaque, job_id: u64) anyerror!?metadata_table_manager.ShuffleJoinLeaseRecord {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.source.getJoinShuffleLease(job_id);
    }

    fn joinCtxUpsertJoinShuffleLease(ptr: *anyopaque, record: metadata_table_manager.ShuffleJoinLeaseRecord) anyerror!void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        _ = try self.source.upsertJoinShuffleLease(record);
    }

    fn joinCtxRemoveJoinShuffleLease(ptr: *anyopaque, job_id: u64) anyerror!void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        _ = try self.source.removeJoinShuffleLease(job_id);
    }

    fn joinCtxExecutePlainQuery(ptr: *anyopaque, alloc: std.mem.Allocator, source: table_reads.TableReadSource, table_name: []const u8, body: []const u8, row_filter_json: ?[]const u8) anyerror!query_api.QueryResponse {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.executePlainPublicTableQuery(alloc, source, table_name, body, row_filter_json);
    }

    fn joinCtxExecuteQueryDispatch(ptr: *anyopaque, alloc: std.mem.Allocator, source: table_reads.TableReadSource, table_name: []const u8, body: []const u8, row_filter_json: ?[]const u8) anyerror![]u8 {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.executePublicTableQueryDispatch(alloc, source, table_name, body, row_filter_json);
    }

    fn joinCtxBuildOwnedSearchRequest(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query_value: std.json.Value) anyerror!query_api.OwnedQueryRequest {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.buildOwnedSearchRequestFromQueryValue(alloc, table_name, query_value);
    }

    fn joinCtxEnsureForeignRegistry(ptr: *anyopaque) anyerror!*const foreign_mod.Registry {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.ensureForeignRegistry();
    }

    pub fn executor(self: *ApiHttpServer) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn streamingExecutor(self: *ApiHttpServer) http_common.StreamingRequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = executeStreaming,
            },
        };
    }

    pub fn runSessionMaintenanceOnce(self: *ApiHttpServer) !void {
        try self.maybeCleanupExpiredSessions();
        try self.maybeRenewOwnedSessionLeases();
        self.join_job_store.cleanupExpiredJoinJobs();
    }

    fn localTableRuntimeStatuses(
        self: *ApiHttpServer,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        return try self.localTableRuntimeStatusesWithSnapshot(table_name, null);
    }

    fn localTableRuntimeStatusesWithSnapshot(
        self: *ApiHttpServer,
        table_name: []const u8,
        snapshot: ?*const metadata_api.AdminSnapshot,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        var items = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (items.items) |*item| item.deinit(self.alloc);
            items.deinit(self.alloc);
        }

        if (self.table_reads) |source| {
            if (try source.localRuntimeStatuses(self.alloc, table_name)) |statuses| {
                var owned = statuses;
                errdefer owned.deinit(self.alloc);
                try self.appendLocalRuntimeStatuses(table_name, snapshot, &items, &owned, .append);
            }
        } else {
            if (self.table_writes) |source| {
                if (try source.localRuntimeStatuses(self.alloc, table_name)) |statuses| {
                    var owned = statuses;
                    errdefer owned.deinit(self.alloc);
                    try self.appendLocalRuntimeStatuses(table_name, snapshot, &items, &owned, .append);
                }
            }
        }
        if (snapshot) |admin_snapshot| {
            try self.appendRemoteRuntimeStatusesFromSnapshot(&items, table_name, admin_snapshot);
        }
        if (items.items.len == 0) {
            items.deinit(self.alloc);
            return null;
        }
        return .{ .items = try items.toOwnedSlice(self.alloc) };
    }

    const LocalRuntimeAppendMode = enum {
        append,
        replace_existing,
    };

    fn appendLocalRuntimeStatuses(
        self: *ApiHttpServer,
        table_name: []const u8,
        snapshot: ?*const metadata_api.AdminSnapshot,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        owned: *runtime_status.LocalTableRuntimeStatuses,
        mode: LocalRuntimeAppendMode,
    ) !void {
        try items.ensureUnusedCapacity(self.alloc, owned.items.len);
        for (owned.items) |item| {
            if (!self.localRuntimeStatusAllowedBySnapshot(table_name, snapshot, item)) {
                var discard = item;
                discard.deinit(self.alloc);
                continue;
            }
            if (localRuntimeStatusGroupIndex(items.items, item.group_id)) |existing_index| {
                switch (mode) {
                    .append => {},
                    .replace_existing => {
                        items.items[existing_index].deinit(self.alloc);
                        items.items[existing_index] = item;
                        continue;
                    },
                }
            }
            items.appendAssumeCapacity(item);
        }
        if (owned.items.len > 0) self.alloc.free(owned.items);
        owned.items = &.{};
    }

    fn localRuntimeStatusGroupIndex(items: []const runtime_status.LocalTableRuntimeStatus, group_id: u64) ?usize {
        for (items, 0..) |item, i| {
            if (item.group_id == group_id) return i;
        }
        return null;
    }

    fn statusAdminSnapshot(self: *ApiHttpServer) !?metadata_api.AdminSnapshot {
        return (try self.source.cachedAdminSnapshot()) orelse try self.source.adminSnapshot();
    }

    fn appendRemoteRuntimeStatusesFromSnapshot(
        self: *ApiHttpServer,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        table_name: []const u8,
        snapshot: *const metadata_api.AdminSnapshot,
    ) !void {
        const table = tables_api.findTableByName(snapshot, table_name) orelse return;
        for (snapshot.stores) |store| {
            for (store.runtime_statuses) |remote| {
                if (remote.table_id != 0 and remote.table_id != table.table_id) continue;
                if (remote.table_name.len != 0 and !std.mem.eql(u8, remote.table_name, table_name)) continue;
                if (!tableHasGroup(snapshot, table.table_id, remote.group_id)) continue;
                if (!storeOwnsRuntimeGroup(snapshot, store, remote.group_id)) continue;

                var status = try localRuntimeStatusFromRemoteReport(self.alloc, remote);
                var consumed = false;
                errdefer if (!consumed) status.deinit(self.alloc);
                try upsertRemoteRuntimeStatus(self.alloc, items, &status);
                consumed = true;
            }
        }
    }

    fn upsertRemoteRuntimeStatus(
        alloc: std.mem.Allocator,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        status: *runtime_status.LocalTableRuntimeStatus,
    ) !void {
        for (items.items) |*existing| {
            if (existing.group_id != status.group_id) continue;
            if (existing.metadata.source != .remote_store) {
                status.deinit(alloc);
                return;
            }
            if (existing.metadata.updated_at_ns >= status.metadata.updated_at_ns) {
                status.deinit(alloc);
                return;
            }
            existing.deinit(alloc);
            existing.* = status.*;
            status.* = undefined;
            return;
        }
        try items.append(alloc, status.*);
        status.* = undefined;
    }

    fn localRuntimeStatusAllowedBySnapshot(
        self: *ApiHttpServer,
        table_name: []const u8,
        maybe_snapshot: ?*const metadata_api.AdminSnapshot,
        status: runtime_status.LocalTableRuntimeStatus,
    ) bool {
        const snapshot = maybe_snapshot orelse return true;
        const table = tables_api.findTableByName(snapshot, table_name) orelse return true;
        if (tableHasAnyGroup(snapshot, table.table_id) and !tableHasGroup(snapshot, table.table_id, status.group_id)) return false;

        const local_node_id = if (status.metadata.node_id != 0)
            status.metadata.node_id
        else if (self.cfg.session_router) |router|
            router.localNodeId()
        else
            0;
        const local_store_id = status.metadata.store_id;

        var saw_group_placement = false;
        for (snapshot.placement_intents) |intent| {
            if (intent.record.group_id != status.group_id) continue;
            saw_group_placement = true;
            if (local_store_id != 0 and intent.store_id == local_store_id) return true;
            if (local_node_id != 0 and intent.record.local_node_id == local_node_id) return true;
            if (local_node_id != 0) {
                for (intent.peer_node_ids) |peer_node_id| {
                    if (peer_node_id == local_node_id) return true;
                }
            }
        }
        if (local_node_id == 0 and local_store_id == 0) return !saw_group_placement or snapshot.stores.len <= 1;
        return !saw_group_placement;
    }

    fn snapshotHasProjectedTableGroups(
        self: *ApiHttpServer,
        table_name: []const u8,
        maybe_snapshot: ?*const metadata_api.AdminSnapshot,
    ) bool {
        _ = self;
        const snapshot = maybe_snapshot orelse return false;
        const table = tables_api.findTableByName(snapshot, table_name) orelse return false;
        return tableHasAnyGroup(snapshot, table.table_id);
    }

    fn localRuntimeStatusFromRemoteReport(
        alloc: std.mem.Allocator,
        report: metadata_table_manager.RuntimeGroupStatusReport,
    ) !runtime_status.LocalTableRuntimeStatus {
        const indexes = try alloc.alloc(db_mod.types.DBIndexStats, report.indexes.len);
        var initialized: usize = 0;
        errdefer {
            for (indexes[0..initialized]) |item| alloc.free(item.name);
            if (indexes.len > 0) alloc.free(indexes);
        }
        for (report.indexes, 0..) |index, i| {
            const kind = parseRemoteIndexKind(index.kind);
            const dense_catch_up_active = kind == .dense_vector and report.async_dense_catch_up_active;
            indexes[i] = .{
                .name = try alloc.dupe(u8, index.name),
                .kind = kind,
                .doc_count = index.doc_count,
                .term_count = index.term_count,
                .edge_count = index.edge_count,
                .node_count = index.node_count,
                .root_node = index.root_node,
                .backfill_active = index.backfill_active,
                .backfill_progress = @as(f64, @floatFromInt(index.backfill_progress_millis)) / 1000.0,
                .replay_applied_sequence = index.replay_applied_sequence,
                .replay_target_sequence = index.replay_target_sequence,
                .replay_catch_up_required = index.replay_catch_up_required,
                .catch_up_active = dense_catch_up_active,
                .catch_up_phase = if (dense_catch_up_active) .replay else .idle,
                .catch_up_applied_sequence = index.replay_applied_sequence,
                .catch_up_target_sequence = index.replay_target_sequence,
            };
            initialized += 1;
        }
        return .{
            .group_id = report.group_id,
            .disk_bytes = report.disk_bytes,
            .created_at_millis = report.created_at_millis,
            .metadata = .{
                .updated_at_ns = report.updated_at_ns,
                .source = .remote_store,
                .freshness = parseRemoteRuntimeFreshness(report.freshness),
                .topology_generation = report.topology_generation,
                .lsm_root_generation = report.lsm_root_generation,
                .status_generation = report.status_generation,
                .store_id = report.store_id,
                .node_id = report.node_id,
            },
            .stats = .{
                .doc_count = report.doc_count,
                .index_count = report.index_count,
                .indexes = indexes,
                .enrichment = .{
                    .enabled = report.enrichment_enabled,
                    .target_sequence = report.enrichment_target_sequence,
                    .applied_sequence = report.enrichment_applied_sequence,
                    .retrying = report.enrichment_retrying,
                    .worker_failed = report.enrichment_worker_failed,
                },
                .async_indexing = .{
                    .startup = .{ .active = report.async_startup_active },
                    .dense_catch_up = .{ .active = report.async_dense_catch_up_active },
                    .bulk_coalescing = .{ .active_session = report.async_bulk_coalescing_active },
                },
                .doc_identity = docIdentityStatsFromRemoteReport(report.doc_identity),
                .doc_set_planning = docSetPlanningStatsFromRemoteReport(report.doc_set_planning),
            },
        };
    }

    fn docIdentityStatsFromRemoteReport(
        report: metadata_table_manager.RuntimeDocIdentityStatusReport,
    ) db_mod.types.DocIdentityStats {
        return .{
            .namespace_table_id = report.namespace_table_id,
            .namespace_shard_id = report.namespace_shard_id,
            .namespace_range_id = report.namespace_range_id,
            .next_ordinal = report.next_ordinal,
            .allocated_ordinals = report.allocated_ordinals,
            .ordinal_capacity_remaining = report.ordinal_capacity_remaining,
            .ordinal_capacity_exhausted = report.ordinal_capacity_exhausted,
            .rebuild_required = report.rebuild_required,
            .state_rows = report.state_rows,
            .live_ordinals = report.live_ordinals,
            .tombstone_ordinals = report.tombstone_ordinals,
            .min_created_generation = report.min_created_generation,
            .max_created_generation = report.max_created_generation,
            .min_deleted_generation = report.min_deleted_generation,
            .max_deleted_generation = report.max_deleted_generation,
            .scanned_primary_docs = report.scanned_primary_docs,
            .primary_docs_missing_ordinals = report.primary_docs_missing_ordinals,
            .primary_docs_missing_identity_state = report.primary_docs_missing_identity_state,
            .primary_docs_with_tombstone_ordinals = report.primary_docs_with_tombstone_ordinals,
            .complete = report.complete,
        };
    }

    fn docSetPlanningStatsFromRemoteReport(
        report: metadata_table_manager.RuntimeDocSetPlanningStatusReport,
    ) db_mod.types.DocSetPlanningStats {
        return .{
            .resolved_set_count = report.resolved_set_count,
            .all_set_count = report.all_set_count,
            .none_set_count = report.none_set_count,
            .doc_key_list_count = report.doc_key_list_count,
            .ordinal_list_count = report.ordinal_list_count,
            .ordinal_bitmap_count = report.ordinal_bitmap_count,
            .doc_key_list_docs = report.doc_key_list_docs,
            .ordinal_list_docs = report.ordinal_list_docs,
            .ordinal_bitmap_docs = report.ordinal_bitmap_docs,
            .missing_ordinal_coverage_count = report.missing_ordinal_coverage_count,
            .bitmap_promotion_count = report.bitmap_promotion_count,
            .unsupported_filter_shape_count = report.unsupported_filter_shape_count,
            .stale_identity_generation_rejection_count = report.stale_identity_generation_rejection_count,
        };
    }

    fn parseRemoteIndexKind(kind: []const u8) db_mod.types.IndexKind {
        if (std.mem.eql(u8, kind, "full_text")) return .full_text;
        if (std.mem.eql(u8, kind, "sparse_vector")) return .sparse_vector;
        if (std.mem.eql(u8, kind, "graph")) return .graph;
        if (std.mem.eql(u8, kind, "algebraic")) return .algebraic;
        return .dense_vector;
    }

    fn parseRemoteRuntimeFreshness(freshness: []const u8) runtime_status.RuntimeStatusFreshness {
        inline for (@typeInfo(runtime_status.RuntimeStatusFreshness).@"enum".fields) |field| {
            if (std.mem.eql(u8, freshness, field.name)) return @enumFromInt(field.value);
        }
        return .remote_unknown;
    }

    fn tableHasGroup(snapshot: *const metadata_api.AdminSnapshot, table_id: u64, group_id: u64) bool {
        for (snapshot.ranges) |range| {
            if (range.table_id == table_id and range.group_id == group_id) return true;
        }
        return false;
    }

    fn tableHasAnyGroup(snapshot: *const metadata_api.AdminSnapshot, table_id: u64) bool {
        for (snapshot.ranges) |range| {
            if (range.table_id == table_id) return true;
        }
        return false;
    }

    fn storeOwnsRuntimeGroup(
        snapshot: *const metadata_api.AdminSnapshot,
        store: metadata_table_manager.StoreRecord,
        group_id: u64,
    ) bool {
        var saw_group_placement = false;
        for (snapshot.placement_intents) |intent| {
            if (intent.record.group_id != group_id) continue;
            saw_group_placement = true;
            if (intent.store_id != 0 and intent.store_id == store.store_id) return true;
            if (intent.record.local_node_id == store.node_id) return true;
            for (intent.peer_node_ids) |peer_node_id| {
                if (peer_node_id == store.node_id) return true;
            }
        }
        return !saw_group_placement;
    }

    pub fn bestEffortSingleTableStorageStatus(
        self: *ApiHttpServer,
        table_name: []const u8,
    ) !?tables_api.TableStorageStatus {
        var local_statuses = (try self.localTableRuntimeStatuses(table_name)) orelse return null;
        defer local_statuses.deinit(self.alloc);

        var doc_count: u64 = 0;
        for (local_statuses.items) |item| doc_count +|= item.stats.doc_count;
        return .{
            .table_name = table_name,
            .empty = doc_count == 0,
        };
    }

    pub fn bestEffortSingleTableStorageStatuses(
        self: *ApiHttpServer,
        table_name: []const u8,
        storage_status_buf: *[1]tables_api.TableStorageStatus,
    ) !?[]const tables_api.TableStorageStatus {
        const status = (try self.bestEffortSingleTableStorageStatus(table_name)) orelse return null;
        storage_status_buf[0] = status;
        return storage_status_buf[0..];
    }

    fn catalogSource(self: *ApiHttpServer) table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = apiHttpServerCatalogAdminSnapshot,
                .free_admin_snapshot = apiHttpServerCatalogFreeAdminSnapshot,
            },
        };
    }

    fn hostedWriteRoutesVisible(self: *ApiHttpServer, table_name: []const u8) !?bool {
        const router = self.cfg.session_router orelse return null;
        var snapshot = (try self.source.adminSnapshot()) orelse return null;
        defer self.source.freeAdminSnapshot(&snapshot);

        const table = tables_api.findTableByName(&snapshot, table_name) orelse return false;
        const ranges = try metadata_admin.listTableRanges(self.alloc, &snapshot, table.table_id);
        defer metadata_admin.freeRangeRefs(self.alloc, ranges);
        if (ranges.len == 0) return false;

        const catalog = self.catalogSource();
        for (ranges) |range| {
            var route = (try table_router.resolveGroupRoute(
                self.alloc,
                catalog,
                router,
                range.group_id,
                .prefer_leader,
            )) orelse return false;
            route.deinit(self.alloc);
        }
        return true;
    }

    pub fn cleanupExpiredSessions(self: *ApiHttpServer, cutoff_ns: u64) !usize {
        return try self.txn_sessions.cleanupExpired(self.alloc, cutoff_ns);
    }

    fn requiresAuthentication(self: *const ApiHttpServer, path: []const u8) bool {
        if (!self.cfg.auth_enabled) return false;
        if (self.cfg.user_manager == null) return false;
        if (std.mem.eql(u8, path, routes.Routes.agent_card) or std.mem.eql(u8, path, routes.Routes.agent_card_legacy)) return false;
        return !std.mem.startsWith(u8, path, routes.Routes.internal_groups_prefix);
    }

    pub fn authenticateRequest(self: *ApiHttpServer, authorization: ?[]const u8) !AuthenticatedIdentity {
        const value = authorization orelse return error.Unauthorized;
        const manager = self.cfg.user_manager orelse return error.Unauthorized;

        if (std.mem.startsWith(u8, value, "Basic ")) {
            const encoded = value["Basic ".len..];
            const raw_size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
            const raw = try self.alloc.alloc(u8, raw_size);
            defer self.alloc.free(raw);
            try std.base64.standard.Decoder.decode(raw, encoded);
            const colon_pos = std.mem.indexOfScalar(u8, raw, ':') orelse return error.Unauthorized;
            var user = try manager.authenticateUser(raw[0..colon_pos], raw[colon_pos + 1 ..]);
            defer user.deinit(self.alloc);
            return .{
                .username = try self.alloc.dupe(u8, user.username),
                .permissions = try manager.getPermissionsForUser(user.username),
                .row_filter = try manager.getRowFilters(user.username),
                .metadata_json = try self.alloc.dupe(u8, user.metadata_json),
                .roles = try manager.getRolesForUser(user.username),
            };
        }

        if (std.mem.startsWith(u8, value, "ApiKey ") or std.mem.startsWith(u8, value, "Bearer ")) {
            const encoded = if (std.mem.startsWith(u8, value, "ApiKey ")) value["ApiKey ".len..] else value["Bearer ".len..];
            const raw_size = try std.base64.standard.Decoder.calcSizeForSlice(encoded);
            const raw = try self.alloc.alloc(u8, raw_size);
            defer self.alloc.free(raw);
            try std.base64.standard.Decoder.decode(raw, encoded);
            const colon_pos = std.mem.indexOfScalar(u8, raw, ':') orelse return error.Unauthorized;
            const validated = try manager.validateApiKey(raw[0..colon_pos], raw[colon_pos + 1 ..]);
            return .{
                .username = validated.username,
                .permissions = validated.permissions,
                .row_filter = validated.row_filter,
                .metadata_json = validated.metadata_json,
                .roles = validated.roles,
            };
        }

        return error.Unauthorized;
    }

    pub fn handleInternalRoute(self: *ApiHttpServer, req: http_common.HttpRequest) !?http_common.HttpResponse {
        const uri_parts = splitTarget(req.uri);
        if (!std.mem.startsWith(u8, uri_parts.path, routes.Routes.internal_groups_prefix) and
            routes.Routes.matchInternalTableCorruptEmbeddingArtifact(uri_parts.path) == null)
        {
            return null;
        }
        try self.runSessionMaintenanceOnce();
        return try http_internal_routes.handle(self.internalRoutesContext(uri_parts), req);
    }

    pub fn handle(self: *ApiHttpServer, req: http_common.HttpRequest) !http_common.HttpResponse {
        self.recordHandledRequest();
        const raw_path = rawPathOnly(req.uri);
        const uri_parts = splitTarget(req.uri);
        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.alloc);

        if (req.method == .GET and std.mem.eql(u8, raw_path, routes.Routes.healthz)) {
            return try jsonResponse(self.alloc, .{ .status = "ok" });
        }
        if (req.method == .GET and std.mem.eql(u8, raw_path, routes.Routes.readyz)) {
            _ = self.source.status() catch {
                return try jsonResponseWithStatus(self.alloc, 503, .{ .status = "not_ready" });
            };
            return try jsonResponse(self.alloc, .{ .status = "ready" });
        }

        if (self.requiresAuthentication(uri_parts.path)) {
            authenticated_identity = self.authenticateRequest(req.authorization) catch |err| switch (err) {
                error.Unauthorized, error.InvalidPassword, error.UserNotFound, error.ApiKeyInvalid, error.ApiKeyNotFound, error.ApiKeyExpired => {
                    return try unauthorizedResponse(self.alloc);
                },
                else => return err,
            };
            const identity = authenticated_identity.?;

            if (requiresAdminPermission(uri_parts.path) and !permissionsAllow(identity.permissions, .@"*", "*", .admin)) {
                return try textResponse(self.alloc, 403, "forbidden");
            }
            if (requiredPermissionForRequest(req.method, uri_parts.path)) |required| {
                if (!permissionsAllow(identity.permissions, required.resource_type, required.resource, required.permission_type)) {
                    return try textResponse(self.alloc, 403, "forbidden");
                }
            }
        }

        if (req.method == .GET and std.mem.eql(u8, uri_parts.path, routes.Routes.status)) {
            const metadata_status = try self.source.status();
            var public_status = try cluster.fromMetadataStatus(self.alloc, metadata_status);
            defer public_status.deinit(self.alloc);
            public_status.auth_enabled = self.cfg.auth_enabled;
            public_status.swarm_mode = self.cfg.swarm_mode;
            if (self.cfg.secret_store) |secret_store| {
                _ = secret_store.refreshIfChanged() catch |err| {
                    std.log.warn("secret store status refresh skipped err={}", .{err});
                };
                cluster.applySecretStoreHealth(&public_status, secret_store.healthSnapshot());
            }
            return try jsonResponse(self.alloc, public_status);
        }
        if (req.method == .GET and std.mem.eql(u8, uri_parts.path, routes.Routes.cluster)) {
            const metadata_status = try self.source.status();
            var public_status = try cluster.fromMetadataStatus(self.alloc, metadata_status);
            defer public_status.deinit(self.alloc);
            public_status.auth_enabled = self.cfg.auth_enabled;
            public_status.swarm_mode = self.cfg.swarm_mode;
            if (self.cfg.secret_store) |secret_store| {
                _ = secret_store.refreshIfChanged() catch |err| {
                    std.log.warn("secret store status refresh skipped err={}", .{err});
                };
                cluster.applySecretStoreHealth(&public_status, secret_store.healthSnapshot());
            }
            var snapshot_opt = try self.source.cachedAdminSnapshot();
            if (snapshot_opt == null) {
                snapshot_opt = try self.source.adminSnapshot();
            }
            if (snapshot_opt) |*snapshot| {
                defer self.source.freeAdminSnapshot(snapshot);
                var topology_status = try cluster.topologyFromStatusAndSnapshot(self.alloc, public_status, snapshot);
                defer topology_status.deinit(self.alloc);
                return try jsonResponse(self.alloc, topology_status);
            }
            var topology_status = try cluster.topologyFromStatus(self.alloc, public_status);
            defer topology_status.deinit(self.alloc);
            return try jsonResponse(self.alloc, topology_status);
        }
        if (try self.dispatchProtocolRoutes(req, uri_parts, authenticated_identity)) |resp| return resp;
        if (try self.dispatchUserRoutes(req, uri_parts, authenticated_identity)) |resp| return resp;
        try self.runSessionMaintenanceOnce();
        if (try self.dispatchSecretRoutes(req, uri_parts)) |resp| return resp;
        if (try self.dispatchTransactionRoutes(req, uri_parts, authenticated_identity)) |resp| return resp;
        if (try http_internal_routes.handle(self.internalRoutesContext(uri_parts), req)) |resp| return resp;
        if (try self.dispatchPublicTableRoutes(req, uri_parts, authenticated_identity)) |resp| return resp;
        return try textResponse(self.alloc, 404, "not found");
    }

    fn dispatchProtocolRoutes(self: *ApiHttpServer, req: http_common.HttpRequest, uri_parts: UriParts, authenticated_identity: ?AuthenticatedIdentity) !?http_common.HttpResponse {
        _ = authenticated_identity;
        if ((req.method == .GET or req.method == .POST or req.method == .DELETE) and (std.mem.eql(u8, uri_parts.path, routes.Routes.mcp_v1) or std.mem.startsWith(u8, uri_parts.path, routes.Routes.mcp_v1_prefix))) {
            return try protocol_adapters.handleMcpRequest(self, req);
        }
        if (req.method == .POST and std.mem.eql(u8, uri_parts.path, routes.Routes.a2a)) {
            return try protocol_adapters.handleA2aRequest(self, req);
        }
        if (req.method == .GET and (std.mem.eql(u8, uri_parts.path, routes.Routes.agent_card_legacy) or std.mem.eql(u8, uri_parts.path, routes.Routes.agent_card))) {
            return try protocol_adapters.handleA2aCard(self);
        }
        return null;
    }

    fn recordHandledRequest(self: *ApiHttpServer) void {
        const prior = self.request_count.fetchAdd(1, .monotonic);
        if (prior == 0) {
            self.first_request_started_at_ns.store(platform_time.monotonicNs(), .monotonic);
        }
    }

    fn dispatchUserRoutes(self: *ApiHttpServer, req: http_common.HttpRequest, uri_parts: UriParts, authenticated_identity: ?AuthenticatedIdentity) !?http_common.HttpResponse {
        if (req.method == .GET and std.mem.eql(u8, uri_parts.path, routes.Routes.users_me)) {
            const identity = authenticated_identity orelse return try unauthorizedResponse(self.alloc);
            var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
            defer arena_impl.deinit();
            const current_user = try makeCurrentUserResponse(arena_impl.allocator(), identity.username, identity.permissions, identity.metadata_json);
            return try jsonResponse(self.alloc, current_user);
        }
        if (req.method == .GET and std.mem.eql(u8, uri_parts.path, routes.Routes.users)) {
            const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
            const users = try manager.listUsers();
            defer freeOwnedStrings(self.alloc, users);
            const listed_users = try makeListedUsers(self.alloc, users);
            defer self.alloc.free(listed_users);
            return try jsonResponse(self.alloc, listed_users);
        }
        if (req.method == .POST) {
            if (routes.Routes.matchUserPath(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                var create_req = parseCreateUserRequest(self.alloc, req.body, user_path.user_name) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid create user request");
                };
                defer create_req.deinit(self.alloc);
                var created = manager.createUserWithMetadata(create_req.username, create_req.password, create_req.initial_policies, create_req.metadata_json) catch |err| switch (err) {
                    error.UserExists => return try jsonErrorResponse(self.alloc, 409, "user already exists"),
                    error.InvalidMetadata => return try jsonErrorResponse(self.alloc, 400, "invalid create user request"),
                    else => return err,
                };
                defer created.deinit(self.alloc);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const generated = try userToOpenApi(arena_impl.allocator(), created);
                return try jsonResponseWithStatus(self.alloc, 201, generated);
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchUserPath(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                var user = manager.getUser(user_path.user_name) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                defer user.deinit(self.alloc);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const generated = try userToOpenApi(arena_impl.allocator(), user);
                return try jsonResponse(self.alloc, generated);
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchUserPath(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                manager.deleteUser(user_path.user_name) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
        }
        if (req.method == .PUT) {
            if (routes.Routes.matchUserPassword(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const new_password = parsePasswordUpdateRequest(self.alloc, req.body) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid password update request");
                };
                defer self.alloc.free(new_password);
                manager.updatePassword(user_path.user_name, new_password) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                return try jsonResponse(self.alloc, .{ .message = "Password updated successfully" });
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchUserPermissions(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const permissions = manager.getPermissionsForUser(user_path.user_name) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                defer freePermissions(self.alloc, permissions);
                const generated_permissions = try clonePermissionsToOpenApi(self.alloc, permissions);
                defer self.alloc.free(generated_permissions);
                return try jsonResponse(self.alloc, generated_permissions);
            }
            if (routes.Routes.matchUserRoles(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const roles = manager.getRolesForUser(user_path.user_name) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                defer freeOwnedStrings(self.alloc, roles);
                return try jsonResponse(self.alloc, roles);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchUserPermissions(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                var permission = parsePermissionBody(self.alloc, req.body) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid permission request");
                };
                defer permission.deinit(self.alloc);
                manager.addPermissionToUser(user_path.user_name, permission) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    error.InvalidPermissionType, error.InvalidResourceType => return try jsonErrorResponse(self.alloc, 400, "invalid permission request"),
                    else => return err,
                };
                return try jsonResponseWithStatus(self.alloc, 201, struct { message: []const u8 }{
                    .message = "Permission added successfully",
                });
            }
            if (routes.Routes.matchUserRoles(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const role = parseRoleAssignmentBody(self.alloc, req.body) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid role request");
                };
                defer self.alloc.free(role);
                manager.addRoleToUser(user_path.user_name, role) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    error.InvalidRole => return try jsonErrorResponse(self.alloc, 400, "invalid role request"),
                    else => return err,
                };
                return try jsonResponseWithStatus(self.alloc, 201, struct { message: []const u8 }{
                    .message = "Role added successfully",
                });
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchUserPermissions(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const params = parseRemovePermissionFromUserParams(uri_parts.query) catch |err| switch (err) {
                    error.MissingResource => return try jsonErrorResponse(self.alloc, 400, "missing resource"),
                    error.MissingResourceType => return try jsonErrorResponse(self.alloc, 400, "missing resourceType"),
                    error.InvalidResourceType => return try jsonErrorResponse(self.alloc, 400, "invalid resourceType"),
                };
                manager.removePermissionFromUser(
                    user_path.user_name,
                    params.resource,
                    usermgr.ResourceType.fromSlice(params.resource_type) catch return try jsonErrorResponse(self.alloc, 400, "invalid resourceType"),
                ) catch |err| switch (err) {
                    error.UserNotFound, error.RoleNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
            if (routes.Routes.matchUserRoles(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const params = parseRemoveRoleFromUserParams(uri_parts.query) catch {
                    return try jsonErrorResponse(self.alloc, 400, "missing role");
                };
                manager.removeRoleFromUser(user_path.user_name, params.role) catch |err| switch (err) {
                    error.UserNotFound, error.RoleNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchUserApiKeys(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const keys = manager.listApiKeys(user_path.user_name) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                defer freeApiKeys(self.alloc, keys);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const arena = arena_impl.allocator();
                const generated = try arena.alloc(usermgr_openapi.ApiKey, keys.len);
                for (keys, 0..) |api_key, i| {
                    generated[i] = try apiKeyToOpenApi(arena, api_key);
                }
                return try jsonResponse(self.alloc, generated);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchUserApiKeys(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                var create_req = parseCreateApiKeyRequest(self.alloc, req.body) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid api key request");
                };
                defer create_req.deinit(self.alloc);
                var created = manager.createApiKey(
                    user_path.user_name,
                    create_req.name,
                    create_req.permissions,
                    create_req.row_filter,
                    create_req.expires_at_ns,
                ) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    error.PrivilegeEscalation => return try jsonErrorResponse(self.alloc, 403, "privilege escalation"),
                    else => return err,
                };
                defer created.deinit(self.alloc);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const generated = try createdApiKeyToOpenApi(arena_impl.allocator(), created);
                return try jsonResponseWithStatus(self.alloc, 201, generated);
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchUserApiKey(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                manager.deleteApiKey(user_path.user_name, user_path.key_id) catch |err| switch (err) {
                    error.ApiKeyNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
        }
        if (req.method == .GET) {
            if (std.mem.eql(u8, uri_parts.path, routes.Routes.auth_subjects)) {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const subjects = try manager.listAuthSubjects();
                defer freeAuthSubjects(self.alloc, subjects);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                return try jsonResponse(self.alloc, try authSubjectsToResponse(arena_impl.allocator(), subjects));
            }
            if (routes.Routes.matchSubjectRowFilters(uri_parts.path)) |subject_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const row_filters = try manager.listSubjectRowFilters(subject_path.subject);
                defer freeRowFilters(self.alloc, row_filters);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const arena = arena_impl.allocator();
                const generated = try arena.alloc(usermgr_openapi.RowFilterEntry, row_filters.len);
                for (row_filters, 0..) |entry, i| {
                    generated[i] = try rowFilterEntryToOpenApi(arena, entry);
                }
                return try jsonResponse(self.alloc, generated);
            }
            if (routes.Routes.matchSubjectRowFilter(uri_parts.path)) |subject_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const filter_json = manager.getSubjectRowFilter(subject_path.subject, subject_path.table) catch |err| switch (err) {
                    error.RowFilterNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                defer self.alloc.free(filter_json);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const generated = try rowFilterEntryToOpenApi(arena_impl.allocator(), .{
                    .table = @constCast(subject_path.table),
                    .filter = @constCast(filter_json),
                });
                return try jsonResponse(self.alloc, generated);
            }
            if (routes.Routes.matchUserRowFilters(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const row_filters = manager.listRowFilters(user_path.user_name) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                defer freeRowFilters(self.alloc, row_filters);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const arena = arena_impl.allocator();
                const generated = try arena.alloc(usermgr_openapi.RowFilterEntry, row_filters.len);
                for (row_filters, 0..) |entry, i| {
                    generated[i] = try rowFilterEntryToOpenApi(arena, entry);
                }
                return try jsonResponse(self.alloc, generated);
            }
            if (routes.Routes.matchUserRowFilter(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                const filter_json = manager.getRowFilter(user_path.user_name, user_path.table) catch |err| switch (err) {
                    error.UserNotFound, error.RowFilterNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                defer self.alloc.free(filter_json);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const generated = try rowFilterEntryToOpenApi(arena_impl.allocator(), .{
                    .table = @constCast(user_path.table),
                    .filter = @constCast(filter_json),
                });
                return try jsonResponse(self.alloc, generated);
            }
        }
        if (req.method == .PUT) {
            if (routes.Routes.matchSubjectRowFilter(uri_parts.path)) |subject_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                var parsed_filter = usermgr_openapi.server.parseSetSubjectRowFilterBody(self.alloc, req.body) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid row filter");
                };
                defer parsed_filter.deinit();
                const normalized_filter = try std.json.Stringify.valueAlloc(self.alloc, parsed_filter.value, .{});
                defer self.alloc.free(normalized_filter);
                validateAuthRowFilterJson(self.alloc, normalized_filter) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid row filter");
                };
                manager.setSubjectRowFilter(subject_path.subject, subject_path.table, normalized_filter) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid row filter");
                };
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const generated = try rowFilterEntryToOpenApi(arena_impl.allocator(), .{
                    .table = @constCast(subject_path.table),
                    .filter = @constCast(normalized_filter),
                });
                return try jsonResponse(self.alloc, generated);
            }
            if (routes.Routes.matchUserRowFilter(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                var parsed_filter = usermgr_openapi.server.parseSetRowFilterBody(self.alloc, req.body) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid row filter");
                };
                defer parsed_filter.deinit();
                const normalized_filter = try std.json.Stringify.valueAlloc(self.alloc, parsed_filter.value, .{});
                defer self.alloc.free(normalized_filter);
                validateAuthRowFilterJson(self.alloc, normalized_filter) catch {
                    return try jsonErrorResponse(self.alloc, 400, "invalid row filter");
                };
                manager.setRowFilter(user_path.user_name, user_path.table, normalized_filter) catch |err| switch (err) {
                    error.UserNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return try jsonErrorResponse(self.alloc, 400, "invalid row filter"),
                };
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const generated = try rowFilterEntryToOpenApi(arena_impl.allocator(), .{
                    .table = @constCast(user_path.table),
                    .filter = @constCast(normalized_filter),
                });
                return try jsonResponse(self.alloc, generated);
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchSubjectRowFilter(uri_parts.path)) |subject_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                manager.removeSubjectRowFilter(subject_path.subject, subject_path.table) catch |err| switch (err) {
                    error.RowFilterNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
            if (routes.Routes.matchUserRowFilter(uri_parts.path)) |user_path| {
                const manager = self.cfg.user_manager orelse return try jsonErrorResponse(self.alloc, 503, "user management not configured");
                manager.removeRowFilter(user_path.user_name, user_path.table) catch |err| switch (err) {
                    error.UserNotFound, error.RowFilterNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
        }
        return null;
    }

    fn dispatchSecretRoutes(self: *ApiHttpServer, req: http_common.HttpRequest, uri_parts: UriParts) !?http_common.HttpResponse {
        if (req.method == .GET and std.mem.eql(u8, uri_parts.path, routes.Routes.secrets)) {
            const listed = if (self.cfg.secret_store) |secret_store|
                try secret_store.list(self.alloc)
            else
                try common_secrets.listEnvironmentSecrets(self.alloc);
            defer common_secrets.freeListedSecrets(self.alloc, listed);
            const secret_list = try makeSecretList(self.alloc, listed);
            defer self.alloc.free(secret_list.secrets);
            return try jsonResponse(self.alloc, secret_list);
        }
        if (req.method == .PUT) {
            if (routes.Routes.matchSecretPath(uri_parts.path)) |secret_path| {
                const secret_store = self.cfg.secret_store orelse return try textResponse(self.alloc, 503, "secret management not available in multi-node mode");
                var parsed = metadata_openapi.server.parsePutSecretBody(self.alloc, req.body) catch {
                    return try textResponse(self.alloc, 400, "invalid secret request");
                };
                defer parsed.deinit();
                var listed = secret_store.put(self.alloc, secret_path.key, parsed.value.value) catch |err| switch (err) {
                    error.InvalidSecretKey => return try textResponse(self.alloc, 400, "invalid secret key"),
                    else => return err,
                };
                defer listed.deinit(self.alloc);
                return try jsonResponse(self.alloc, makeSecretEntry(listed));
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchSecretPath(uri_parts.path)) |secret_path| {
                const secret_store = self.cfg.secret_store orelse return try textResponse(self.alloc, 503, "secret management not available in multi-node mode");
                if (!(try secret_store.delete(secret_path.key))) {
                    return try textResponse(self.alloc, 404, "not found");
                }
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
        }
        return null;
    }

    fn dispatchTransactionRoutes(self: *ApiHttpServer, req: http_common.HttpRequest, uri_parts: UriParts, authenticated_identity: ?AuthenticatedIdentity) !?http_common.HttpResponse {
        if (req.method == .GET) {
            if (std.mem.eql(u8, uri_parts.path, routes.Routes.transactions)) {
                const sessions = try self.txn_sessions.listStatuses(self.alloc);
                defer self.alloc.free(sessions);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionListResponse(arena_impl.allocator(), sessions);
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST and std.mem.eql(u8, uri_parts.path, routes.Routes.transactions_cleanup)) {
            const now_ns = platform_time.realtimeNs();
            const cutoff_ns = if (try parseUnsignedQueryParam(uri_parts.query, "cutoff_ns")) |explicit_cutoff|
                explicit_cutoff
            else if (self.cfg.session_ttl_ns) |ttl_ns|
                now_ns -| ttl_ns
            else
                return try textResponse(self.alloc, 400, "missing cutoff");
            const removed = try self.cleanupExpiredSessions(cutoff_ns);
            return try jsonResponse(self.alloc, transactions_api.buildSessionCleanupResponse(removed, cutoff_ns));
        }
        if (req.method == .POST and std.mem.eql(u8, uri_parts.path, routes.Routes.eval)) {
            var parsed = metadata_openapi.server.parseEvaluateBody(self.alloc, req.body) catch {
                return try jsonErrorResponse(self.alloc, 400, "invalid eval request");
            };
            defer parsed.deinit();
            var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
            defer arena_impl.deinit();
            const response = retrieval_agent.buildEvalResponse(arena_impl.allocator(), parsed.value) catch |err| switch (err) {
                error.InvalidEvalRequest => return try jsonErrorResponse(self.alloc, 400, "invalid eval request"),
                else => return err,
            };
            return try jsonResponse(self.alloc, response);
        }
        if (req.method == .POST and std.mem.eql(u8, uri_parts.path, routes.Routes.agents_query_builder)) {
            var parsed = metadata_openapi.server.parseQueryBuilderAgentBody(self.alloc, req.body) catch {
                return try jsonErrorResponse(self.alloc, 400, "invalid query builder request");
            };
            defer parsed.deinit();
            if (parsed.value.intent.len == 0) return try jsonErrorResponse(self.alloc, 400, "invalid query builder request");

            var table_context: ?query_builder_agent.QueryBuilderTableContext = null;
            defer if (table_context) |context| freeQueryBuilderTableContext(self.alloc, context);
            var runtime_validator_context: ?QueryBuilderRuntimeQueryRequestValidatorContext = null;
            if (parsed.value.table) |table_name| {
                if (self.cfg.auth_enabled) {
                    const identity = authenticated_identity orelse return try unauthorizedResponse(self.alloc);
                    if (!permissionsAllow(identity.permissions, .table, table_name, .read)) {
                        return try jsonErrorResponse(self.alloc, 403, "forbidden");
                    }
                }
                table_context = self.loadQueryBuilderTableContext(table_name) catch |err| switch (err) {
                    error.TableNotFound => return try jsonErrorResponse(self.alloc, 404, "not found"),
                    else => return err,
                };
                if (self.table_reads) |reads| {
                    runtime_validator_context = .{
                        .server = self,
                        .source = reads,
                        .table_name = table_name,
                    };
                    table_context.?.runtime_query_request_validator = runtime_validator_context.?.iface();
                }
            }

            var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
            defer arena_impl.deinit();
            const QueryBuilderGenerationRunner = struct {
                local_termite_provider: ?managed_embedder.LocalTermiteProvider,
                secret_store: ?*common_secrets.FileStore,

                fn iface(runner: *@This()) query_builder_agent.GenerationRunner {
                    return .{
                        .ptr = runner,
                        .vtable = &.{ .execute_chain = executeChain },
                    };
                }

                fn executeChain(
                    ptr: *anyopaque,
                    alloc: std.mem.Allocator,
                    chain: []const generating_runtime.ChainLink,
                    messages: []const generating_runtime.ChatMessage,
                ) !generating_runtime.GenerateResult {
                    const runner: *@This() = @ptrCast(@alignCast(ptr));
                    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                    defer io_impl.deinit();
                    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
                    defer client.deinit();
                    return try generating_runtime.executeChainWithOptions(alloc, &client, chain, .{ .local_termite_provider = runner.local_termite_provider, .secret_store = runner.secret_store }, messages);
                }
            };
            var generation_runner = QueryBuilderGenerationRunner{ .local_termite_provider = self.local_termite_provider, .secret_store = self.cfg.secret_store };
            var collected_context = query_builder_agent.collectQueryBuilderContext(table_context);
            const response = query_builder_agent.buildQueryBuilderResponseWithCollectedContext(arena_impl.allocator(), parsed.value, &collected_context, generation_runner.iface()) catch |err| switch (err) {
                error.InvalidQueryBuilderRequest => return try jsonErrorResponse(self.alloc, 400, "invalid query builder request"),
                error.DocIdentityNamespaceMismatch => return try jsonErrorResponse(self.alloc, 503, "doc identity unavailable"),
                else => return err,
            };
            return try jsonResponse(self.alloc, response);
        }
        if (req.method == .GET) {
            if (routes.Routes.matchTransactionSession(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                var details = (self.txn_sessions.getDetails(self.alloc, txn_id) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");
                defer details.deinit(self.alloc);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSessionDetailsResponse(arena_impl.allocator(), details);
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST) {
            if (std.mem.eql(u8, uri_parts.path, routes.Routes.transactions_begin)) {
                const begin_req = transactions_api.parseBeginRequest(self.alloc, req.body) catch {
                    return try textResponse(self.alloc, 400, "invalid transaction begin request");
                };
                const session = try self.txn_sessions.begin(self.alloc, begin_req, self.localSessionNodeId());
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildBeginResponse(arena_impl.allocator(), session);
                return try jsonResponseWithStatus(self.alloc, 201, response);
            }
        }
        if (req.method == .POST) {
            if (std.mem.eql(u8, uri_parts.path, routes.Routes.transactions_commit)) {
                const source = self.table_writes orelse return try textResponse(self.alloc, 404, "not found");
                var commit_req = transactions_api.parseCommitRequest(self.alloc, req.body) catch |err| switch (err) {
                    error.InvalidTransactionCommitRequest => {
                        return try textResponse(self.alloc, 400, "invalid transaction commit request");
                    },
                    else => return err,
                };
                defer commit_req.deinit(self.alloc);

                const distributed_tables = try commit_req.distributedTables(self.alloc);
                defer if (distributed_tables.len > 0) self.alloc.free(distributed_tables);
                self.validateCommitTablesAgainstSchema(distributed_tables) catch |err| switch (err) {
                    error.InvalidBatchRequest => return try textResponse(self.alloc, 400, "invalid transaction commit request"),
                    else => return err,
                };
                if (try self.validateCommitReadSet(commit_req)) |conflict| {
                    var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                    defer arena_impl.deinit();
                    const response = try transactions_api.buildCommitResponse(
                        arena_impl.allocator(),
                        "aborted",
                        conflict,
                        null,
                    );
                    return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                }

                const outcome = (source.commitTransaction(self.alloc, distributed_tables, commit_req.sync_level) catch |err| switch (err) {
                    error.InvalidBatchRequest => return try textResponse(self.alloc, 400, "invalid transaction commit request"),
                    error.TopologyChanged => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildCommitResponse(
                            arena_impl.allocator(),
                            "aborted",
                            transactions_api.topologyChangedConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    error.DecisionConflict => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildCommitResponse(
                            arena_impl.allocator(),
                            "aborted",
                            transactions_api.decisionConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    error.DocIdentityNamespaceMismatch => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildCommitResponse(
                            arena_impl.allocator(),
                            "aborted",
                            transactions_api.docIdentityUnavailableConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    error.TxnNotFound, error.InvalidTxnRecord => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildCommitResponse(
                            arena_impl.allocator(),
                            "aborted",
                            transactions_api.tornStateConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    error.UnsupportedOperation => return try textResponse(self.alloc, 405, "method not allowed"),
                    error.TableNotFound, error.UnknownGroup => return try textResponse(self.alloc, 404, "not found"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");

                switch (outcome) {
                    .committed => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildCommitResponse(arena_impl.allocator(), "committed", null, commit_req.tables);
                        return try jsonResponseOmitNullOptionals(self.alloc, response);
                    },
                    .conflict => |conflict| {
                        const enriched_conflict = try self.enrichCommitConflict(commit_req, conflict);
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildCommitResponse(
                            arena_impl.allocator(),
                            "aborted",
                            enriched_conflict,
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                }
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionRead(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                var read_req = transactions_api.parseStageReadPayload(self.alloc, req.body) catch {
                    return try textResponse(self.alloc, 400, "invalid transaction read request");
                };
                defer read_req.deinit(self.alloc);

                var owned_snapshot: transactions_api.SessionReadSnapshot = (self.txn_sessions.getReadSnapshot(self.alloc, txn_id, read_req.table_name, read_req.key) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    else => return err,
                }) orelse .{
                    .table_name = try self.alloc.dupe(u8, read_req.table_name),
                    .key = try self.alloc.dupe(u8, read_req.key),
                    .version = 0,
                    .document_json = null,
                };
                defer owned_snapshot.deinit(self.alloc);

                if (owned_snapshot.version == 0 and self.table_reads != null) {
                    const fetched = try self.lookupStageReadSnapshot(read_req.table_name, read_req.key);
                    if (owned_snapshot.document_json) |document_json| self.alloc.free(document_json);
                    self.alloc.free(owned_snapshot.table_name);
                    self.alloc.free(owned_snapshot.key);
                    owned_snapshot = .{
                        .table_name = try self.alloc.dupe(u8, fetched.table_name),
                        .key = try self.alloc.dupe(u8, fetched.key),
                        .version = fetched.version,
                        .document_json = if (fetched.document_json) |document_json| try self.alloc.dupe(u8, document_json) else null,
                    };
                    if (fetched.document_json) |document_json| self.alloc.free(document_json);
                } else if (owned_snapshot.version == 0) {
                    owned_snapshot.version = read_req.version;
                }
                if (owned_snapshot.version != read_req.version) {
                    var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                    defer arena_impl.deinit();
                    const response = try transactions_api.buildSessionCommitResponse(
                        arena_impl.allocator(),
                        txn_id,
                        "conflict",
                        transactions_api.versionConflict(read_req.table_name, read_req.key, read_req.version, owned_snapshot.version),
                        null,
                    );
                    return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                }

                var stage_req = try transactions_api.ownedRequestFromStageReadRequest(self.alloc, read_req);
                defer stage_req.deinit(self.alloc);

                const session = (self.txn_sessions.stageRead(self.alloc, txn_id, &stage_req, owned_snapshot.stage()) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildStageReadResponse(arena_impl.allocator(), session.txn_id, owned_snapshot.stage());
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionWrite(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                var stage_req = transactions_api.parseStageWriteRequest(self.alloc, req.body) catch {
                    return try textResponse(self.alloc, 400, "invalid transaction write request");
                };
                defer stage_req.deinit(self.alloc);

                const session = (self.txn_sessions.stage(self.alloc, txn_id, &stage_req) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildStageResponse(arena_impl.allocator(), session.txn_id);
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionDelete(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                var stage_req = transactions_api.parseStageDeleteRequest(self.alloc, req.body) catch {
                    return try textResponse(self.alloc, 400, "invalid transaction delete request");
                };
                defer stage_req.deinit(self.alloc);

                const session = (self.txn_sessions.stage(self.alloc, txn_id, &stage_req) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildStageResponse(arena_impl.allocator(), session.txn_id);
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionSavepoints(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                const info = (self.txn_sessions.createSavepoint(self.alloc, txn_id) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    error.SavepointLimitExceeded => return try textResponse(self.alloc, 409, "savepoint limit exceeded"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildSavepointResponse(arena_impl.allocator(), info);
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionRollback(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                const info = (self.txn_sessions.rollbackToSavepoint(self.alloc, txn_id, session_route.savepoint_id) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildRollbackResponse(arena_impl.allocator(), info);
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionStage(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                var stage_req = transactions_api.parseCommitRequest(self.alloc, req.body) catch |err| switch (err) {
                    error.InvalidTransactionCommitRequest => {
                        return try textResponse(self.alloc, 400, "invalid transaction stage request");
                    },
                    else => return err,
                };
                defer stage_req.deinit(self.alloc);

                const session = (self.txn_sessions.stage(self.alloc, txn_id, &stage_req) catch |err| switch (err) {
                    error.SessionLeaseLost => return try textResponse(self.alloc, 409, "session lease lost"),
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildStageResponse(arena_impl.allocator(), session.txn_id);
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionCommit(uri_parts.path)) |session_route| {
                const source = self.table_writes orelse return try textResponse(self.alloc, 404, "not found");
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                const session = self.txn_sessions.getInfo(txn_id) orelse return try textResponse(self.alloc, 404, "not found");

                var parsed_req: ?transactions_api.OwnedTransactionCommitRequest = null;
                defer if (parsed_req) |*commit_req| commit_req.deinit(self.alloc);
                if (!transactions_api.isEmptySessionCommitBody(req.body)) {
                    parsed_req = transactions_api.parseCommitRequest(self.alloc, req.body) catch |err| switch (err) {
                        error.InvalidTransactionCommitRequest => {
                            return try textResponse(self.alloc, 400, "invalid transaction commit request");
                        },
                        else => return err,
                    };
                }
                var commit_req = (self.txn_sessions.cloneCommitRequest(self.alloc, txn_id, if (parsed_req) |*value| value else null) catch |err| switch (err) {
                    error.SessionLeaseLost => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildSessionCommitResponse(
                            arena_impl.allocator(),
                            txn_id,
                            "aborted",
                            transactions_api.sessionLeaseLostConflict(if (parsed_req) |value| if (value.tables.len > 0) value.tables[0].table_name else "" else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    else => return err,
                }) orelse {
                    return try textResponse(self.alloc, 400, "transaction has no staged writes");
                };
                defer commit_req.deinit(self.alloc);

                const distributed_tables = try commit_req.distributedTables(self.alloc);
                defer if (distributed_tables.len > 0) self.alloc.free(distributed_tables);
                self.validateCommitTablesAgainstSchema(distributed_tables) catch |err| switch (err) {
                    error.InvalidBatchRequest => return try textResponse(self.alloc, 400, "invalid transaction commit request"),
                    else => return err,
                };
                if (try self.validateCommitReadSet(commit_req)) |conflict| {
                    _ = self.txn_sessions.remove(self.alloc, txn_id);
                    var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                    defer arena_impl.deinit();
                    const response = try transactions_api.buildSessionCommitResponse(
                        arena_impl.allocator(),
                        txn_id,
                        "aborted",
                        conflict,
                        null,
                    );
                    return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                }

                const outcome = (source.commitTransactionWithId(self.alloc, txn_id, session.begin_timestamp, distributed_tables, session.sync_level) catch |err| switch (err) {
                    error.InvalidBatchRequest => return try textResponse(self.alloc, 400, "invalid transaction commit request"),
                    error.TopologyChanged => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildSessionCommitResponse(
                            arena_impl.allocator(),
                            txn_id,
                            "aborted",
                            transactions_api.topologyChangedConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    error.DecisionConflict => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildSessionCommitResponse(
                            arena_impl.allocator(),
                            txn_id,
                            "aborted",
                            transactions_api.decisionConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    error.DocIdentityNamespaceMismatch => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildSessionCommitResponse(
                            arena_impl.allocator(),
                            txn_id,
                            "aborted",
                            transactions_api.docIdentityUnavailableConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    error.UnsupportedOperation => return try textResponse(self.alloc, 405, "method not allowed"),
                    error.TableNotFound => return try textResponse(self.alloc, 404, "not found"),
                    error.UnknownGroup => {
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildSessionCommitResponse(
                            arena_impl.allocator(),
                            txn_id,
                            "aborted",
                            transactions_api.participantUnavailableConflict(if (commit_req.tables.len > 0) commit_req.tables[0].table_name else ""),
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                    else => return err,
                }) orelse return try textResponse(self.alloc, 404, "not found");

                switch (outcome) {
                    .committed => {
                        _ = self.txn_sessions.remove(self.alloc, txn_id);
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildSessionCommitResponse(arena_impl.allocator(), txn_id, "committed", null, commit_req.tables);
                        return try jsonResponseOmitNullOptionals(self.alloc, response);
                    },
                    .conflict => |conflict| {
                        _ = self.txn_sessions.remove(self.alloc, txn_id);
                        const enriched_conflict = try self.enrichCommitConflict(commit_req, conflict);
                        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                        defer arena_impl.deinit();
                        const response = try transactions_api.buildSessionCommitResponse(
                            arena_impl.allocator(),
                            txn_id,
                            "aborted",
                            enriched_conflict,
                            null,
                        );
                        return try jsonResponseWithStatusOmitNullOptionals(self.alloc, 409, response);
                    },
                }
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTransactionSessionAbort(uri_parts.path)) |session_route| {
                const txn_id = distributed_txn.parseTxnIdHex(session_route.txn_id) catch return try textResponse(self.alloc, 400, "invalid transaction id");
                if (try self.forwardSessionRequest(txn_id, req)) |resp| return resp;
                if (!self.txn_sessions.remove(self.alloc, txn_id)) return try textResponse(self.alloc, 404, "not found");
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = try transactions_api.buildAbortResponse(arena_impl.allocator(), txn_id);
                return try jsonResponse(self.alloc, response);
            }
        }
        return null;
    }

    fn internalRoutesContext(self: *ApiHttpServer, uri_parts: UriParts) http_internal_routes.Context {
        return .{
            .alloc = self.alloc,
            .path = uri_parts.path,
            .query = uri_parts.query,
            .read_ctx = .{
                .alloc = self.alloc,
                .reads = self.table_reads,
                .catalog = .{
                    .ptr = self.source.ptr,
                    .admin_snapshot = self.source.vtable.admin_snapshot,
                    .free_admin_snapshot = self.source.vtable.free_admin_snapshot,
                },
                .query_router = .{
                    .ptr = self,
                    .route_query_to_read_schema = routeInternalGroupQueryToReadSchema,
                },
            },
            .join_ctx = self.joinContext(),
            .join_job_store = &self.join_job_store,
            .write_ctx = .{
                .alloc = self.alloc,
                .shard_ops = self.cfg.shard_ops,
                .shard_db_adapter = self.cfg.shard_db_adapter,
                .writes = self.table_writes,
                .batch_validator = .{
                    .ptr = self,
                    .validate = validateInternalGroupBatchWrites,
                },
                .txn_validator = .{
                    .ptr = self,
                    .validate = validateInternalGroupTxnWrites,
                },
            },
            .retrieval_executor = .{
                .ptr = self,
                .execute = executeInternalRetrievalRoute,
            },
        };
    }

    pub fn executeA2aRetrieval(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        body: []const u8,
        task_id: []const u8,
        context_id: []const u8,
        queue: *a2a.EventQueue,
    ) !void {
        const source = self.table_reads orelse {
            try queue.status(alloc, task_id, context_id, "failed", "not found");
            return;
        };

        const RetrievalQueryRunner = struct {
            server: *ApiHttpServer,
            source: table_reads.TableReadSource,

            fn iface(runner: *@This()) retrieval_agent.QueryRunner {
                return .{
                    .ptr = runner,
                    .vtable = &.{
                        .run_query = runQuery,
                        .scan_keys = scanKeys,
                    },
                };
            }

            fn runQuery(
                ptr_inner: *anyopaque,
                inner_alloc: std.mem.Allocator,
                table_name: []const u8,
                query_json: []const u8,
            ) !query_api.QueryResponse {
                const runner: *@This() = @ptrCast(@alignCast(ptr_inner));
                var semantic_resolver = SemanticStatusResolver{
                    .source = runner.server.source,
                    .local_termite_provider = runner.server.local_termite_provider,
                    .remote_content = runner.server.cfg.remote_content,
                };
                var query_req = query_api.parsePublicQueryRequest(inner_alloc, semantic_resolver.iface(), table_name, query_json) catch |err| switch (err) {
                    error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidRetrievalAgentRequest,
                    else => return err,
                };
                defer query_req.deinit(inner_alloc);
                runner.server.maybeRouteQueryToReadSchema(table_name, &query_req.req) catch |err| switch (err) {
                    error.TableNotFound => return err,
                    error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return error.InvalidRetrievalAgentRequest,
                    else => return err,
                };
                return (runner.source.query(
                    inner_alloc,
                    table_name,
                    query_req.req,
                    .read_index,
                ) catch |err| {
                    if (err == error.DocIdentityNamespaceMismatch) return err;
                    std.log.err("retrieval query failed table={s} query={s} err={}", .{ table_name, query_json, err });
                    return err;
                }) orelse error.TableNotFound;
            }

            fn scanKeys(
                ptr_inner: *anyopaque,
                inner_alloc: std.mem.Allocator,
                table_name: []const u8,
            ) ![]const []const u8 {
                const runner: *@This() = @ptrCast(@alignCast(ptr_inner));
                var scan = (try runner.source.scan(
                    inner_alloc,
                    table_name,
                    "",
                    "",
                    .{ .limit = 0 },
                    .read_index,
                )) orelse return error.TableNotFound;
                defer scan.deinit(inner_alloc);

                var keys = std.ArrayListUnmanaged([]const u8).empty;
                errdefer {
                    for (keys.items) |key| inner_alloc.free(key);
                    keys.deinit(inner_alloc);
                }

                var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
                while (lines.next()) |line| {
                    if (line.len == 0) continue;
                    const key = scanLineKey(inner_alloc, line) catch return error.InvalidRetrievalAgentRequest;
                    try keys.append(inner_alloc, key);
                }
                return try keys.toOwnedSlice(inner_alloc);
            }
        };

        const RetrievalGenerationRunner = struct {
            local_termite_provider: ?managed_embedder.LocalTermiteProvider,
            secret_store: ?*common_secrets.FileStore,

            fn iface(runner: *@This()) retrieval_agent.GenerationRunner {
                return .{
                    .ptr = runner,
                    .vtable = &.{ .execute_chain = executeChain },
                };
            }

            fn executeChain(
                runner_ptr: *anyopaque,
                inner_alloc: std.mem.Allocator,
                chain: []const generating_runtime.ChainLink,
                messages: []const generating_runtime.ChatMessage,
            ) !generating_runtime.GenerateResult {
                const runner: *@This() = @ptrCast(@alignCast(runner_ptr));
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                var client = httpx.Client.initWithConfig(inner_alloc, io_impl.io(), .{ .keep_alive = false });
                defer client.deinit();
                return try generating_runtime.executeChainWithOptions(inner_alloc, &client, chain, .{ .local_termite_provider = runner.local_termite_provider, .secret_store = runner.secret_store }, messages);
            }
        };
        var generation_runner = RetrievalGenerationRunner{ .local_termite_provider = self.local_termite_provider, .secret_store = self.cfg.secret_store };

        var query_runner = RetrievalQueryRunner{
            .server = self,
            .source = source,
        };

        const RetrievalA2aSink = struct {
            queue: *a2a.EventQueue,
            task_id: []const u8,
            context_id: []const u8,

            fn iface(sink: *@This()) retrieval_agent.EventSink {
                return .{ .ptr = sink, .emit_json_fn = emitJson };
            }

            fn emitJson(ptr_inner: *anyopaque, event_alloc: std.mem.Allocator, event_name: []const u8, json: []const u8) !void {
                const sink: *@This() = @ptrCast(@alignCast(ptr_inner));
                const parsed: std.json.Value = std.json.parseFromSliceLeaky(std.json.Value, event_alloc, json, .{}) catch .{ .string = try event_alloc.dupe(u8, json) };

                if (std.mem.eql(u8, event_name, "error")) {
                    try sink.queue.status(event_alloc, sink.task_id, sink.context_id, "failed", json);
                    return;
                }

                const artifact_name = if (std.mem.eql(u8, event_name, "done")) "result" else event_name;
                var payload = std.json.ObjectMap.empty;
                try payload.put(event_alloc, "event", .{ .string = event_name });
                try payload.put(event_alloc, "data", parsed);
                try sink.queue.artifact(event_alloc, sink.task_id, sink.context_id, artifact_name, try a2a.dataPart(event_alloc, .{ .object = payload }));

                if (std.mem.eql(u8, event_name, "done")) {
                    const status = if (parsed == .object) parsed.object.get("status") else null;
                    const state = if (status) |value| switch (value) {
                        .string => |text| text,
                        else => "completed",
                    } else "completed";
                    try sink.queue.status(event_alloc, sink.task_id, sink.context_id, state, "retrieval completed");
                }
            }
        };

        var sink = RetrievalA2aSink{
            .queue = queue,
            .task_id = task_id,
            .context_id = context_id,
        };
        try queue.status(alloc, task_id, context_id, "working", "retrieval started");
        const retrieval_resp = retrieval_agent.executeWithEventSink(alloc, query_runner.iface(), generation_runner.iface(), body, sink.iface()) catch |err| switch (err) {
            error.InvalidRetrievalAgentRequest, error.UnsupportedRetrievalAgentRequest => {
                try queue.status(alloc, task_id, context_id, "failed", "invalid retrieval agent request");
                return;
            },
            error.TableNotFound => {
                try queue.status(alloc, task_id, context_id, "failed", "not found");
                return;
            },
            error.DocIdentityNamespaceMismatch => {
                try queue.status(alloc, task_id, context_id, "failed", "doc identity unavailable");
                return;
            },
            else => {
                std.log.err("public retrieval failed err={}", .{err});
                return err;
            },
        };
        defer alloc.free(retrieval_resp.body);
    }

    fn executeInternalRetrievalRoute(ptr: *anyopaque, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        if (req.method != .POST or !std.mem.eql(u8, path, routes.Routes.agents_retrieval)) return null;

        const source = self.table_reads orelse return try textResponse(self.alloc, 404, "not found");

        const RetrievalQueryRunner = struct {
            server: *ApiHttpServer,
            source: table_reads.TableReadSource,

            fn iface(runner: *@This()) retrieval_agent.QueryRunner {
                return .{
                    .ptr = runner,
                    .vtable = &.{
                        .run_query = runQuery,
                        .scan_keys = scanKeys,
                    },
                };
            }

            fn runQuery(
                ptr_inner: *anyopaque,
                alloc: std.mem.Allocator,
                table_name: []const u8,
                query_json: []const u8,
            ) !query_api.QueryResponse {
                const runner: *@This() = @ptrCast(@alignCast(ptr_inner));
                var semantic_resolver = SemanticStatusResolver{
                    .source = runner.server.source,
                    .local_termite_provider = runner.server.local_termite_provider,
                    .remote_content = runner.server.cfg.remote_content,
                };
                var query_req = query_api.parseQueryRequest(alloc, semantic_resolver.iface(), table_name, query_json) catch |err| switch (err) {
                    error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidRetrievalAgentRequest,
                    else => return err,
                };
                defer query_req.deinit(alloc);
                runner.server.maybeRouteQueryToReadSchema(table_name, &query_req.req) catch |err| switch (err) {
                    error.TableNotFound => return err,
                    error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return error.InvalidRetrievalAgentRequest,
                    else => return err,
                };
                return (runner.source.query(
                    alloc,
                    table_name,
                    query_req.req,
                    .read_index,
                ) catch |err| {
                    if (err == error.DocIdentityNamespaceMismatch) return err;
                    std.log.err("retrieval query failed table={s} query={s} err={}", .{ table_name, query_json, err });
                    return err;
                }) orelse error.TableNotFound;
            }

            fn scanKeys(
                ptr_inner: *anyopaque,
                alloc: std.mem.Allocator,
                table_name: []const u8,
            ) ![]const []const u8 {
                const runner: *@This() = @ptrCast(@alignCast(ptr_inner));
                var scan = (try runner.source.scan(
                    alloc,
                    table_name,
                    "",
                    "",
                    .{ .limit = 0 },
                    .read_index,
                )) orelse return error.TableNotFound;
                defer scan.deinit(alloc);

                var keys = std.ArrayListUnmanaged([]const u8).empty;
                errdefer {
                    for (keys.items) |key| alloc.free(key);
                    keys.deinit(alloc);
                }

                var lines = std.mem.splitScalar(u8, scan.ndjson, '\n');
                while (lines.next()) |line| {
                    if (line.len == 0) continue;
                    const key = scanLineKey(alloc, line) catch return error.InvalidRetrievalAgentRequest;
                    try keys.append(alloc, key);
                }
                return try keys.toOwnedSlice(alloc);
            }
        };

        const RetrievalGenerationRunner = struct {
            local_termite_provider: ?managed_embedder.LocalTermiteProvider,
            secret_store: ?*common_secrets.FileStore,

            fn iface(runner: *@This()) retrieval_agent.GenerationRunner {
                return .{
                    .ptr = runner,
                    .vtable = &.{ .execute_chain = executeChain },
                };
            }

            fn executeChain(
                runner_ptr: *anyopaque,
                alloc: std.mem.Allocator,
                chain: []const generating_runtime.ChainLink,
                messages: []const generating_runtime.ChatMessage,
            ) !generating_runtime.GenerateResult {
                const runner: *@This() = @ptrCast(@alignCast(runner_ptr));
                var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
                defer io_impl.deinit();
                var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{ .keep_alive = false });
                defer client.deinit();
                return try generating_runtime.executeChainWithOptions(alloc, &client, chain, .{ .local_termite_provider = runner.local_termite_provider, .secret_store = runner.secret_store }, messages);
            }
        };
        var generation_runner = RetrievalGenerationRunner{ .local_termite_provider = self.local_termite_provider, .secret_store = self.cfg.secret_store };

        var query_runner = RetrievalQueryRunner{
            .server = self,
            .source = source,
        };
        const retrieval_resp = retrieval_agent.execute(self.alloc, query_runner.iface(), generation_runner.iface(), req.body) catch |err| switch (err) {
            error.InvalidRetrievalAgentRequest, error.UnsupportedRetrievalAgentRequest => return try textResponse(self.alloc, 400, "invalid retrieval agent request"),
            error.TableNotFound => return try textResponse(self.alloc, 404, "not found"),
            error.DocIdentityNamespaceMismatch => return try textResponse(self.alloc, 503, "doc identity unavailable"),
            else => {
                std.log.err("public retrieval failed err={}", .{err});
                return err;
            },
        };
        defer self.alloc.free(retrieval_resp.body);
        if (std.mem.eql(u8, retrieval_resp.content_type, "text/event-stream")) {
            return try eventStreamResponse(self.alloc, 200, retrieval_resp.body);
        }
        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
        defer arena_impl.deinit();
        const response = try parseJsonResponseBody(metadata_openapi.RetrievalAgentResult, arena_impl.allocator(), retrieval_resp.body);
        return try jsonResponse(self.alloc, response);
    }

    fn dispatchPublicTableRoutes(self: *ApiHttpServer, req: http_common.HttpRequest, uri_parts: UriParts, authenticated_identity: ?AuthenticatedIdentity) !?http_common.HttpResponse {
        if (req.method == .GET and std.mem.eql(u8, uri_parts.path, routes.Routes.backups)) {
            const params = parseListBackupsParams(uri_parts.query) catch return try textResponse(self.alloc, 400, "missing location");
            return try self.handlePublicClusterBackupList(params.location);
        }
        if (req.method == .GET and std.mem.eql(u8, uri_parts.path, routes.Routes.tables)) {
            var snapshot = (try self.source.adminSnapshot()) orelse return try textResponse(self.alloc, 404, "not found");
            defer self.source.freeAdminSnapshot(&snapshot);
            const params = try parseListTablesParams(uri_parts.query);
            if (params.pattern != null) return try textResponse(self.alloc, 400, "unsupported table pattern");
            const storage_statuses = try self.collectTableStorageStatuses(&snapshot, params.prefix);
            defer if (storage_statuses) |items| self.alloc.free(items);
            var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
            defer arena_impl.deinit();
            const response = try tables_api.buildTableListWithStorageStatuses(arena_impl.allocator(), &snapshot, params.prefix, storage_statuses);
            return try jsonResponse(self.alloc, response);
        }
        if (req.method == .GET) {
            if (routes.Routes.matchTableIndexes(uri_parts.path)) |table_indexes| {
                return try self.handlePublicTableListIndexes(table_indexes.table_name);
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchTableIndex(uri_parts.path)) |table_index| {
                if (runtimeSchemaDebugRequested(uri_parts.query)) {
                    if (!self.runtimeSchemaDebugAllowed(authenticated_identity)) return try textResponse(self.alloc, 403, "forbidden");
                    var snapshot = (try self.source.adminSnapshot()) orelse return try textResponse(self.alloc, 404, "not found");
                    defer self.source.freeAdminSnapshot(&snapshot);
                    var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                    defer arena_impl.deinit();
                    const arena = arena_impl.allocator();
                    const table = tables_api.findTableByName(&snapshot, table_index.table_name) orelse return try textResponse(self.alloc, 404, "not found");
                    var local_statuses = self.localTableRuntimeStatusesWithSnapshot(table_index.table_name, &snapshot) catch return try textResponse(self.alloc, 500, "index lookup failed");
                    defer if (local_statuses) |*status| status.deinit(self.alloc);
                    const body = (indexes_api.encodeSingleIndex(
                        arena,
                        &snapshot,
                        table_index.table_name,
                        table_index.index_name,
                        if (local_statuses) |*status| status else null,
                    ) catch return try textResponse(self.alloc, 500, "index lookup failed")) orelse return try textResponse(self.alloc, 404, "not found");
                    var value = parseOwnedJsonValueAlloc(arena, body) catch return try textResponse(self.alloc, 500, "index lookup failed");
                    if (value != .object) return try textResponse(self.alloc, 500, "index lookup failed");
                    try value.object.put(arena, try arena.dupe(u8, "debug"), try tables_api.buildTableIndexRuntimeSchemaDebugValue(arena, table, table_index.index_name));
                    return try jsonResponse(self.alloc, value);
                }
                return try self.handlePublicTableGetIndex(table_index.table_name, table_index.index_name);
            }
        }
        if (req.method == .POST and std.mem.eql(u8, uri_parts.path, routes.Routes.backup)) {
            return try self.handlePublicClusterBackup(req.body);
        }
        if (req.method == .POST) {
            if (std.mem.eql(u8, uri_parts.path, routes.Routes.restore)) {
                return try self.handlePublicClusterRestore(req.body);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTableBackup(uri_parts.path)) |table_backup| {
                return try self.handlePublicTableBackup(table_backup.table_name, req.body);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTableRestore(uri_parts.path)) |table_restore| {
                return try self.handlePublicTableRestore(table_restore.table_name, req.body);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTableIndex(uri_parts.path)) |table_index| {
                return try self.handlePublicTableCreateIndex(table_index.table_name, table_index.index_name, req.body);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTablePath(uri_parts.path)) |table_path| {
                var create_req = table_contract.parseCreateTableRequest(self.alloc, req.body) catch |err| {
                    std.log.err("create table parse failed: {} body_len={d}", .{ err, req.body.len });
                    return try textResponse(self.alloc, 400, "invalid create table request");
                };
                defer create_req.deinit(self.alloc);
                const normalized_indexes_json = table_writes.normalizeManagedEmbeddingIndexDimensionsJsonWithOptions(
                    self.alloc,
                    create_req.indexes_json orelse tables_api.default_indexes_json,
                    .{
                        .local_termite_provider = self.local_termite_provider,
                        .secret_store = self.cfg.secret_store,
                        .remote_content = self.cfg.remote_content,
                    },
                ) catch |err| switch (err) {
                    error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported table index configuration"),
                    else => return err,
                };
                if (create_req.indexes_json) |old_indexes_json| self.alloc.free(old_indexes_json);
                create_req.indexes_json = normalized_indexes_json;
                tables_api.validatePublicAlgebraicIndexesJson(self.alloc, create_req.indexes_json orelse tables_api.default_indexes_json) catch {
                    return try textResponse(self.alloc, 400, "unsupported table index configuration");
                };
                std.log.info("public create table begin table={s}", .{table_path.table_name});
                const metadata_create_timeout_ns = 5 * std.time.ns_per_s;
                const metadata_create_poll_ns = 50 * std.time.ns_per_ms;
                const metadata_create_start_ns = platform_time.monotonicNs();
                while (true) {
                    self.source.createTable(self.alloc, table_path.table_name, create_req) catch |err| switch (err) {
                        error.UnsupportedOperation => return try textResponse(self.alloc, 405, "method not allowed"),
                        error.UnexpectedHttpStatus => {
                            if (platform_time.monotonicNs() -| metadata_create_start_ns >= metadata_create_timeout_ns) {
                                std.log.err("public create table metadata create failed table={s} err={}", .{ table_path.table_name, err });
                                return err;
                            }
                            sleepNs(metadata_create_poll_ns);
                            continue;
                        },
                        else => {
                            std.log.err("public create table metadata create failed table={s} err={}", .{ table_path.table_name, err });
                            return err;
                        },
                    };
                    break;
                }
                std.log.info("public create table metadata done table={s}", .{table_path.table_name});
                const local_create_handled = if (self.table_writes) |table_writes_source| blk: {
                    break :blk (table_writes_source.createTable(self.alloc, table_path.table_name, create_req) catch |err| switch (err) {
                        error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return try textResponse(self.alloc, 400, "unsupported table index configuration"),
                        else => {
                            std.log.err("public create table local create failed table={s} err={}", .{ table_path.table_name, err });
                            return err;
                        },
                    }) != null;
                } else false;
                if (local_create_handled) {
                    std.log.info("public create table wait projected presence table={s}", .{table_path.table_name});
                    self.waitForProjectedTablePresence(table_path.table_name) catch |err| switch (err) {
                        error.TableVisibilityTimeout => {
                            std.log.err("public create table metadata visibility timed out table={s}", .{table_path.table_name});
                            return try textResponse(self.alloc, 500, "table create did not converge");
                        },
                        else => return err,
                    };
                    self.waitForProjectedTableWriteQuorum(table_path.table_name) catch |err| switch (err) {
                        error.TableVisibilityTimeout => {
                            std.log.err("public create table write quorum timed out table={s}", .{table_path.table_name});
                            return try textResponse(self.alloc, 500, "table create did not converge");
                        },
                        else => return err,
                    };
                } else {
                    const metadata_wait_handled = self.source.waitTableLifecycle(table_path.table_name, .present) catch |err| lifecycle: {
                        break :lifecycle switch (err) {
                            error.TableVisibilityTimeout => {
                                self.waitForProjectedTableCreateReadiness(table_path.table_name) catch |fallback_err| switch (fallback_err) {
                                    error.TableVisibilityTimeout => {
                                        std.log.err("public create table metadata lifecycle timed out table={s}", .{table_path.table_name});
                                        return try textResponse(self.alloc, 500, "table create did not converge");
                                    },
                                    else => return fallback_err,
                                };
                                break :lifecycle true;
                            },
                            else => {
                                std.log.err("public create table metadata lifecycle failed table={s} err={}", .{ table_path.table_name, err });
                                return err;
                            },
                        };
                    };
                    if (!metadata_wait_handled) {
                        std.log.info("public create table wait metadata visibility table={s}", .{table_path.table_name});
                        self.waitForTableVisibility(table_path.table_name, .present) catch |err| switch (err) {
                            error.TableVisibilityTimeout => {
                                std.log.err("public create table metadata visibility timed out table={s}", .{table_path.table_name});
                                return try textResponse(self.alloc, 500, "table create did not converge");
                            },
                            else => return err,
                        };
                    }
                }
                std.log.info("public create table visible table={s}", .{table_path.table_name});

                var snapshot = (try self.source.adminSnapshot()) orelse return try textResponse(self.alloc, 404, "not found");
                defer self.source.freeAdminSnapshot(&snapshot);
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = (try tables_api.buildSingleTableStatusWithStorageStatuses(arena_impl.allocator(), &snapshot, table_path.table_name, null)) orelse {
                    return try textResponse(self.alloc, 404, "not found");
                };
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .PUT) {
            if (routes.Routes.matchTableSchema(uri_parts.path)) |table_schema| {
                const schema_json = table_contract.parseSchemaUpdateRequest(self.alloc, req.body) catch {
                    return try textResponse(self.alloc, 400, "invalid schema update request");
                };
                defer self.alloc.free(schema_json);

                const table_before = try self.loadOwnedTableRecord(table_schema.table_name);
                if (table_before == null) {
                    self.source.updateSchema(self.alloc, table_schema.table_name, schema_json) catch |err| switch (err) {
                        error.InvalidSchemaUpdateRequest => return try textResponse(self.alloc, 400, "invalid schema update request"),
                        error.TableNotFound => return try textResponse(self.alloc, 404, "not found"),
                        error.UnsupportedOperation => {
                            const table_writes_source = self.table_writes orelse return try textResponse(self.alloc, 404, "not found");
                            _ = table_writes_source.updateSchema(self.alloc, table_schema.table_name, schema_json) catch |write_err| switch (write_err) {
                                error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => return try textResponse(self.alloc, 400, "invalid schema update request"),
                                else => return write_err,
                            } orelse return try textResponse(self.alloc, 404, "not found");
                        },
                        else => return err,
                    };
                    var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                    defer arena_impl.deinit();
                    const value = try buildLocalSchemaUpdateStatus(arena_impl.allocator(), table_schema.table_name, schema_json);
                    return try jsonResponse(self.alloc, value);
                }
                defer metadata_table_manager.freeTable(self.alloc, table_before.?);

                var local_schema_applied = false;
                self.source.updateSchema(self.alloc, table_schema.table_name, schema_json) catch |err| switch (err) {
                    error.InvalidSchemaUpdateRequest => return try textResponse(self.alloc, 400, "invalid schema update request"),
                    error.TableNotFound => return try textResponse(self.alloc, 404, "not found"),
                    error.UnsupportedOperation => {
                        const table_writes_source = self.table_writes orelse return try textResponse(self.alloc, 405, "method not allowed");
                        _ = table_writes_source.updateSchema(self.alloc, table_schema.table_name, schema_json) catch |write_err| switch (write_err) {
                            error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => return try textResponse(self.alloc, 400, "invalid schema update request"),
                            else => return write_err,
                        };
                        local_schema_applied = true;
                    },
                    else => return err,
                };
                const expected_table = try tables_api.applySchemaUpdateRecord(self.alloc, &table_before.?, schema_json);
                defer metadata_table_manager.freeTable(self.alloc, expected_table);
                self.waitForMetadataProjection(table_schema.table_name, expected_table.schema_json, expected_table.indexes_json) catch |err| switch (err) {
                    error.TableVisibilityTimeout => return try textResponse(self.alloc, 500, "schema update did not converge"),
                    else => return err,
                };
                if (self.table_writes) |table_writes_source| {
                    if (!local_schema_applied) {
                        _ = table_writes_source.updateSchema(self.alloc, table_schema.table_name, schema_json) catch |write_err| switch (write_err) {
                            error.InvalidSchemaUpdateRequest, error.InvalidCreateTableRequest => return try textResponse(self.alloc, 400, "invalid schema update request"),
                            else => return write_err,
                        };
                    }
                    if (try self.source.runRound()) {
                        _ = try self.source.runRound();
                        _ = try self.source.runRound();
                    }
                }

                const body = try self.encodeSchemaUpdateResponse(table_schema.table_name, schema_json);
                defer self.alloc.free(body);
                return try jsonBodyResponseWithStatus(self.alloc, 200, body);
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchTableIndex(uri_parts.path)) |table_index| {
                return try self.handlePublicTableDeleteIndex(table_index.table_name, table_index.index_name);
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchTableLookup(uri_parts.path)) |lookup| {
                const source = self.table_reads orelse return try textResponse(self.alloc, 404, "not found");
                const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(self.alloc, lookup.key);
                defer self.alloc.free(decoded_key);
                var lookup_opts = try http_route_helpers.parseLookupOptions(self.alloc, uri_parts.query);
                defer lookup_opts.deinit(self.alloc);

                var result = (try source.lookup(self.alloc, lookup.table_name, decoded_key, lookup_opts.opts, .read_index)) orelse {
                    return try textResponse(self.alloc, 404, "not found");
                };
                defer result.deinit(self.alloc);
                const row_filter_json = try resolveEffectiveRowFilterJson(self.alloc, authenticated_identity, lookup.table_name);
                defer if (row_filter_json) |value| self.alloc.free(value);
                if (row_filter_json) |value| {
                    if (!(try self.docMatchesRowFilter(source, lookup.table_name, decoded_key, value))) {
                        return try textResponse(self.alloc, 404, "not found");
                    }
                }
                return try http_route_helpers.jsonWithHeadersResponse(self.alloc, 200, result.json, &.{
                    .{
                        .name = "X-Antfly-Version",
                        .value = try std.fmt.allocPrint(self.alloc, "{d}", .{result.version}),
                    },
                });
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchTablePath(uri_parts.path)) |table_path| {
                if (runtimeSchemaDebugRequested(uri_parts.query) and !self.runtimeSchemaDebugAllowed(authenticated_identity)) {
                    return try textResponse(self.alloc, 403, "forbidden");
                }
                var snapshot = (try self.source.adminSnapshot()) orelse return try textResponse(self.alloc, 404, "not found");
                defer self.source.freeAdminSnapshot(&snapshot);
                var storage_status_buf: [1]tables_api.TableStorageStatus = undefined;
                const storage_statuses = try self.bestEffortSingleTableStorageStatuses(table_path.table_name, &storage_status_buf);
                if (runtimeSchemaDebugRequested(uri_parts.query)) {
                    var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                    defer arena_impl.deinit();
                    const response =
                        (try tables_api.buildSingleTableStatusWithRuntimeSchemaDebug(arena_impl.allocator(), &snapshot, table_path.table_name, storage_statuses)) orelse return try textResponse(self.alloc, 404, "not found");
                    return try jsonResponse(self.alloc, response);
                }
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = (try tables_api.buildSingleTableStatusWithStorageStatuses(arena_impl.allocator(), &snapshot, table_path.table_name, storage_statuses)) orelse return try textResponse(self.alloc, 404, "not found");
                return try jsonResponse(self.alloc, response);
            }
        }
        if (req.method == .DELETE) {
            if (routes.Routes.matchTablePath(uri_parts.path)) |table_path| {
                var local_drop_group_ids: ?[]u64 = null;
                defer if (local_drop_group_ids) |group_ids| self.alloc.free(group_ids);
                if (self.table_writes != null) {
                    if (try self.source.adminSnapshot()) |snapshot_value| {
                        var snapshot = snapshot_value;
                        defer self.source.freeAdminSnapshot(&snapshot);
                        local_drop_group_ids = try tableGroupIdsFromSnapshot(self.alloc, &snapshot, table_path.table_name);
                    }
                }
                self.source.dropTable(self.alloc, table_path.table_name) catch |err| switch (err) {
                    error.TableNotFound => return try textResponse(self.alloc, 404, "not found"),
                    error.UnsupportedOperation => return try textResponse(self.alloc, 405, "method not allowed"),
                    else => {
                        std.log.err("public drop table metadata remove failed table={s} err={s}", .{
                            table_path.table_name,
                            @errorName(err),
                        });
                        return err;
                    },
                };
                if (self.table_writes) |write_source| {
                    const group_ids = local_drop_group_ids orelse &.{};
                    _ = write_source.dropTable(self.alloc, table_path.table_name, group_ids) catch |err| switch (err) {
                        error.TableNotFound => null,
                        else => {
                            std.log.err("public drop table local cleanup failed table={s} err={s}", .{
                                table_path.table_name,
                                @errorName(err),
                            });
                            return err;
                        },
                    };
                }
                self.waitForTableVisibility(table_path.table_name, .absent) catch |err| switch (err) {
                    error.TableVisibilityTimeout => {
                        std.log.err("public drop table metadata visibility timed out table={s}", .{table_path.table_name});
                        return try textResponse(self.alloc, 500, "table delete did not converge");
                    },
                    else => return err,
                };
                return .{
                    .status = 204,
                    .content_type = null,
                    .body = &.{},
                };
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTableScan(uri_parts.path)) |scan| {
                const source = self.table_reads orelse return try textResponse(self.alloc, 404, "not found");
                var scan_req = try http_route_helpers.parseScanKeysRequest(self.alloc, req.body);
                defer scan_req.deinit(self.alloc);

                var result = (try source.scan(
                    self.alloc,
                    scan.table_name,
                    scan_req.from,
                    scan_req.to,
                    scan_req.opts,
                    .read_index,
                )) orelse return try textResponse(self.alloc, 404, "not found");
                defer result.deinit(self.alloc);
                const row_filter_json = try resolveEffectiveRowFilterJson(self.alloc, authenticated_identity, scan.table_name);
                defer if (row_filter_json) |value| self.alloc.free(value);
                if (row_filter_json) |value| {
                    const filtered = try self.filterScanResultByRowFilter(source, scan.table_name, result.ndjson, value);
                    defer self.alloc.free(filtered);
                    return try http_route_helpers.ndjsonResponse(self.alloc, 200, filtered);
                }
                return try http_route_helpers.ndjsonResponse(self.alloc, 200, result.ndjson);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTableQuery(uri_parts.path)) |query_route| {
                return try self.handlePublicTableQuery(query_route.table_name, req.body, authenticated_identity);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTableBatch(uri_parts.path)) |batch_route| {
                return try self.handlePublicTableBatch(batch_route.table_name, req.body);
            }
        }
        if (req.method == .POST) {
            if (routes.Routes.matchTableMerge(uri_parts.path)) |merge_route| {
                const reads = self.table_reads orelse return try textResponse(self.alloc, 404, "not found");
                const writes = self.table_writes orelse return try textResponse(self.alloc, 404, "not found");
                if (!(try self.tableExists(merge_route.table_name))) return try textResponse(self.alloc, 404, "not found");

                var merge_req = linear_merge_api.parseRequest(self.alloc, req.body) catch |err| switch (err) {
                    error.InvalidLinearMergeRequest => return try textResponse(self.alloc, 400, "invalid linear merge request"),
                    else => return err,
                };
                defer merge_req.deinit(self.alloc);

                self.validateTableWritesAgainstSchema(merge_route.table_name, merge_req.writes) catch |err| switch (err) {
                    error.InvalidBatchRequest => return try textResponse(self.alloc, 400, "invalid linear merge request"),
                    else => return err,
                };

                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const response = linear_merge_api.executeResponse(
                    arena_impl.allocator(),
                    reads,
                    writes,
                    merge_route.table_name,
                    merge_req,
                ) catch |err| switch (err) {
                    error.InvalidLinearMergeRequest => return try textResponse(self.alloc, 400, "invalid linear merge request"),
                    error.TableNotFound => return try textResponse(self.alloc, 404, "not found"),
                    error.InvalidBatchRequest => return try textResponse(self.alloc, 400, "invalid linear merge request"),
                    else => return err,
                };
                return try jsonResponse(self.alloc, response);
            }
        }
        return null;
    }

    pub fn localSessionNodeId(self: *ApiHttpServer) u64 {
        const router = self.cfg.session_router orelse return 0;
        return router.localNodeId();
    }

    fn maybeCleanupExpiredSessions(self: *ApiHttpServer) !void {
        const ttl_ns = self.cfg.session_ttl_ns orelse return;
        const now_ns = platform_time.realtimeNs();
        const interval_ns = self.cfg.session_cleanup_interval_ns orelse ttl_ns;
        if (self.last_session_cleanup_ns != 0 and now_ns -| self.last_session_cleanup_ns < interval_ns) return;
        self.last_session_cleanup_ns = now_ns;
        _ = try self.txn_sessions.cleanupExpired(self.alloc, now_ns -| ttl_ns);
    }

    fn routeInternalGroupQueryToReadSchema(ptr: *anyopaque, table_name: []const u8, req: *db_mod.types.SearchRequest) !void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.maybeRouteQueryToReadSchema(table_name, req);
    }

    fn validateInternalGroupBatchWrites(ptr: *anyopaque, table_name: []const u8, writes: []const db_mod.types.BatchWrite) !void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.validateTableWritesAgainstSchema(table_name, writes);
    }

    fn validateInternalGroupTxnWrites(ptr: *anyopaque, table_name: []const u8, writes: []const db_mod.types.TransactionWrite) !void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.validateTableWritesAgainstSchema(table_name, writes);
    }

    pub fn maybeRouteQueryToReadSchema(self: *ApiHttpServer, table_name: []const u8, query_req: *db_mod.types.SearchRequest) !void {
        var snapshot = (try self.source.adminSnapshot()) orelse return;
        defer self.source.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        try tables_api.routeQueryRequestToActiveReadIndex(self.alloc, table, query_req);
    }

    pub fn validateTableWritesAgainstSchema(self: *ApiHttpServer, table_name: []const u8, writes: anytype) !void {
        if (writes.len == 0) return;
        var snapshot = (try self.source.adminSnapshot()) orelse return;
        defer self.source.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        if (table.schema_json.len == 0) return;

        var parsed_schema = try tables_api.parseValidatedTableSchema(self.alloc, table.schema_json);
        defer parsed_schema.deinit(self.alloc);
        try tables_api.validateWritesAgainstTableSchema(self.alloc, parsed_schema, writes);
    }

    pub fn validateCommitTablesAgainstSchema(self: *ApiHttpServer, tables: []const distributed_txn.TableCommitRequest) !void {
        for (tables) |table| try self.validateTableWritesAgainstSchema(table.table_name, table.writes);
    }

    pub fn validateCommitReadSet(
        self: *ApiHttpServer,
        req: transactions_api.OwnedTransactionCommitRequest,
    ) !?transactions_api.CommitConflict {
        const source = self.table_reads orelse return null;
        for (req.read_set) |item| {
            var lookup = (try source.lookup(self.alloc, item.table_name, item.key, .{}, .read_index)) orelse {
                return transactions_api.versionConflict(item.table_name, item.key, item.expected_version, 0);
            };
            defer lookup.deinit(self.alloc);
            if (lookup.version != item.expected_version) {
                return transactions_api.versionConflict(item.table_name, item.key, item.expected_version, lookup.version);
            }
        }
        return null;
    }

    pub fn maybeEncodeTableStatus(self: *ApiHttpServer, table_name: []const u8) !?[]u8 {
        var snapshot = (try self.source.adminSnapshot()) orelse return null;
        defer self.source.freeAdminSnapshot(&snapshot);
        var storage_status_buf: [1]tables_api.TableStorageStatus = undefined;
        const storage_statuses = try self.bestEffortSingleTableStorageStatuses(table_name, &storage_status_buf);
        return try tables_api.encodeSingleTableStatusWithStorageStatuses(self.alloc, &snapshot, table_name, storage_statuses);
    }

    pub fn encodeSchemaUpdateResponse(self: *ApiHttpServer, table_name: []const u8, schema_json: []const u8) ![]u8 {
        if (try self.maybeEncodeTableStatus(table_name)) |body| return body;

        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
        defer arena_impl.deinit();
        const value = try buildLocalSchemaUpdateStatus(arena_impl.allocator(), table_name, schema_json);
        return try std.json.Stringify.valueAlloc(self.alloc, value, .{});
    }

    pub fn probeTableStorageStatus(self: *ApiHttpServer, table_name: []const u8) !?tables_api.TableStorageStatus {
        const source = self.table_reads orelse return null;
        var result = (try source.scan(
            self.alloc,
            table_name,
            "",
            "",
            .{ .limit = 1 },
            .read_index,
        )) orelse return null;
        defer result.deinit(self.alloc);
        return .{
            .table_name = table_name,
            .empty = result.ndjson.len == 0,
        };
    }

    pub fn collectTableStorageStatuses(
        self: *ApiHttpServer,
        snapshot: *const metadata_api.AdminSnapshot,
        prefix: ?[]const u8,
    ) !?[]tables_api.TableStorageStatus {
        var items = std.ArrayListUnmanaged(tables_api.TableStorageStatus).empty;
        defer items.deinit(self.alloc);

        for (snapshot.tables) |*table| {
            if (prefix) |pfx| {
                if (!std.mem.startsWith(u8, table.name, pfx)) continue;
            }
            const status = (try self.bestEffortSingleTableStorageStatus(table.name)) orelse continue;
            try items.append(self.alloc, status);
        }

        if (items.items.len == 0) return null;
        return try items.toOwnedSlice(self.alloc);
    }

    pub fn tableGroupIdsFromSnapshot(
        alloc: std.mem.Allocator,
        snapshot: *const metadata_api.AdminSnapshot,
        table_name: []const u8,
    ) ![]u64 {
        const table = tables_api.findTableByName(snapshot, table_name) orelse return try alloc.alloc(u64, 0);
        const ranges = try metadata_admin.listTableRanges(alloc, snapshot, table.table_id);
        defer metadata_admin.freeRangeRefs(alloc, ranges);

        const group_ids = try alloc.alloc(u64, ranges.len);
        for (ranges, 0..) |range, i| group_ids[i] = range.group_id;
        return group_ids;
    }

    pub fn loadOwnedTableRecord(self: *ApiHttpServer, table_name: []const u8) !?metadata_table_manager.TableRecord {
        var snapshot = (try self.source.adminSnapshot()) orelse return null;
        defer self.source.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
        return try metadata_table_manager.cloneTable(self.alloc, table.*);
    }

    pub fn docMatchesRowFilter(
        self: *ApiHttpServer,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        key: []const u8,
        row_filter_json: []const u8,
    ) !bool {
        var response = (try source.lookup(self.alloc, table_name, key, .{}, .read_index)) orelse return false;
        defer response.deinit(self.alloc);
        return try search_pattern_filter.storedDocMatchesPatternFilter(self.alloc, key, response.json, row_filter_json);
    }

    pub fn filterScanResultByRowFilter(
        self: *ApiHttpServer,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        ndjson: []const u8,
        row_filter_json: []const u8,
    ) ![]u8 {
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.alloc);

        var lines = std.mem.splitScalar(u8, ndjson, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            const key = try scanLineKey(self.alloc, line);
            defer self.alloc.free(key);
            if (!(try self.docMatchesRowFilter(source, table_name, key, row_filter_json))) continue;
            try out.appendSlice(self.alloc, line);
            try out.append(self.alloc, '\n');
        }

        return try out.toOwnedSlice(self.alloc);
    }

    fn loadOwnedTableNames(self: *ApiHttpServer) ![]const []const u8 {
        var snapshot = (try self.source.adminSnapshot()) orelse return try self.alloc.alloc([]const u8, 0);
        defer self.source.freeAdminSnapshot(&snapshot);
        const names = try self.alloc.alloc([]const u8, snapshot.tables.len);
        var initialized: usize = 0;
        errdefer {
            for (names[0..initialized]) |name| self.alloc.free(@constCast(name));
            self.alloc.free(names);
        }
        for (snapshot.tables, 0..) |table, i| {
            names[i] = try self.alloc.dupe(u8, table.name);
            initialized += 1;
        }
        return names;
    }

    pub fn tableExists(self: *ApiHttpServer, table_name: []const u8) !bool {
        var snapshot = (try self.source.adminSnapshot()) orelse return false;
        defer self.source.freeAdminSnapshot(&snapshot);
        return tables_api.findTableByName(&snapshot, table_name) != null;
    }

    pub fn loadQueryBuilderTableSchemaFields(self: *ApiHttpServer, table_name: []const u8) ![]const []const u8 {
        var context = try self.loadQueryBuilderTableContext(table_name);
        const schema_fields = context.schema_fields;
        context.schema_fields = &.{};
        freeQueryBuilderTableContext(self.alloc, context);
        return schema_fields;
    }

    pub fn loadQueryBuilderTableContext(self: *ApiHttpServer, table_name: []const u8) !query_builder_agent.QueryBuilderTableContext {
        var snapshot = (try self.source.adminSnapshot()) orelse return error.TableNotFound;
        defer self.source.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        const schema_fields = try self.loadQueryBuilderSchemaFieldsFromJson(table.schema_json);
        errdefer freeOwnedStrings(self.alloc, schema_fields);
        const index_context = try self.loadQueryBuilderIndexContextFromJson(table.indexes_json);
        errdefer freeQueryBuilderIndexContext(self.alloc, index_context);
        return .{
            .schema_fields = schema_fields,
            .full_text_index_metadata = index_context.full_text_index_metadata,
            .embedding_index_metadata = index_context.embedding_index_metadata,
            .graph_index_metadata = index_context.graph_index_metadata,
        };
    }

    fn loadQueryBuilderSchemaFieldsFromJson(self: *ApiHttpServer, schema_json: []const u8) ![]const []const u8 {
        if (schema_json.len == 0) return try self.alloc.alloc([]const u8, 0);
        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
        defer arena_impl.deinit();
        const arena = arena_impl.allocator();

        const parsed_schema = try tables_api.parseValidatedTableSchema(arena, schema_json);
        const runtime_schema = try schema_mod.deriveRuntimeTableSchema(arena, parsed_schema);

        var seen = std.StringHashMapUnmanaged(void).empty;
        defer seen.deinit(arena);

        var fields = std.ArrayListUnmanaged([]const u8).empty;
        errdefer {
            for (fields.items) |field| self.alloc.free(@constCast(field));
            fields.deinit(self.alloc);
        }

        for (runtime_schema.full_text_documents) |document_schema| {
            for (document_schema.fields) |field| {
                if (seen.contains(field.path)) continue;
                try seen.put(arena, field.path, {});
                try fields.append(self.alloc, try self.alloc.dupe(u8, field.path));
            }
        }

        return try fields.toOwnedSlice(self.alloc);
    }

    fn loadQueryBuilderIndexContextFromJson(self: *ApiHttpServer, indexes_json: []const u8) !QueryBuilderIndexContext {
        const source = if (indexes_json.len == 0) "{}" else indexes_json;
        var parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, source, .{});
        defer parsed.deinit();
        const object = switch (parsed.value) {
            .object => |object| object,
            else => return error.InvalidTableIndexMetadata,
        };

        var full_text_metadata = std.ArrayListUnmanaged(query_builder_agent.QueryBuilderFullTextIndex).empty;
        defer full_text_metadata.deinit(self.alloc);
        var embedding_metadata = std.ArrayListUnmanaged(query_builder_agent.QueryBuilderEmbeddingIndex).empty;
        defer embedding_metadata.deinit(self.alloc);
        var graph_metadata = std.ArrayListUnmanaged(query_builder_agent.QueryBuilderGraphIndex).empty;
        defer graph_metadata.deinit(self.alloc);

        errdefer {
            freeQueryBuilderFullTextIndexMetadataItems(self.alloc, full_text_metadata.items);
            freeQueryBuilderEmbeddingIndexMetadataItems(self.alloc, embedding_metadata.items);
            freeQueryBuilderGraphIndexMetadataItems(self.alloc, graph_metadata.items);
        }

        var it = object.iterator();
        while (it.next()) |entry| {
            const index_type = queryBuilderIndexContextType(entry.key_ptr.*, entry.value_ptr.*) orelse continue;
            switch (index_type) {
                .full_text => {
                    try self.appendQueryBuilderFullTextIndexMetadata(&full_text_metadata, entry.key_ptr.*, entry.value_ptr.*);
                },
                .embeddings => {
                    const metadata = try self.queryBuilderEmbeddingIndexMetadata(entry.key_ptr.*, entry.value_ptr.*);
                    errdefer freeQueryBuilderEmbeddingIndexMetadataItem(self.alloc, metadata);
                    try embedding_metadata.append(self.alloc, metadata);
                },
                .graph => {
                    try self.appendQueryBuilderGraphIndexMetadata(&graph_metadata, entry.key_ptr.*, entry.value_ptr.*);
                },
            }
        }

        var context = QueryBuilderIndexContext{};
        errdefer freeQueryBuilderIndexContext(self.alloc, context);
        context.full_text_index_metadata = if (full_text_metadata.items.len == 0) &.{} else try full_text_metadata.toOwnedSlice(self.alloc);
        context.embedding_index_metadata = if (embedding_metadata.items.len == 0) &.{} else try embedding_metadata.toOwnedSlice(self.alloc);
        context.graph_index_metadata = if (graph_metadata.items.len == 0) &.{} else try graph_metadata.toOwnedSlice(self.alloc);
        return context;
    }

    fn queryBuilderIndexContextType(index_name: []const u8, value: std.json.Value) ?QueryBuilderIndexContextType {
        if (value == .object) {
            if (queryBuilderJsonString(value.object.get("type"))) |type_name| {
                if (std.mem.eql(u8, type_name, "full_text")) return .full_text;
                if (std.mem.eql(u8, type_name, "embeddings") or
                    std.mem.eql(u8, type_name, "dense_vector") or
                    std.mem.eql(u8, type_name, "sparse_vector"))
                {
                    return .embeddings;
                }
                if (std.mem.eql(u8, type_name, "graph")) return .graph;
                return null;
            }
        }
        return if (indexes_api.inferIndexType(index_name, value)) |index_type| switch (index_type) {
            .full_text => .full_text,
            .embeddings => .embeddings,
            .graph => .graph,
            .algebraic => null,
        } else null;
    }

    fn appendQueryBuilderFullTextIndexMetadata(
        self: *ApiHttpServer,
        metadata: *std.ArrayListUnmanaged(query_builder_agent.QueryBuilderFullTextIndex),
        index_name: []const u8,
        value: std.json.Value,
    ) !void {
        const name = try self.alloc.dupe(u8, index_name);
        errdefer self.alloc.free(name);
        const fields = try self.loadQueryBuilderFullTextIndexFields(value);
        errdefer freeOwnedStrings(self.alloc, fields);
        const entry = query_builder_agent.QueryBuilderFullTextIndex{
            .name = name,
            .fields = fields,
        };
        try metadata.append(self.alloc, entry);
    }

    fn loadQueryBuilderFullTextIndexFields(self: *ApiHttpServer, value: std.json.Value) ![]const []const u8 {
        const object = switch (value) {
            .object => |object| object,
            else => return &.{},
        };
        var fields = std.ArrayListUnmanaged([]const u8).empty;
        defer fields.deinit(self.alloc);
        errdefer freeOwnedStringItems(self.alloc, fields.items);

        if (object.get("field")) |field_value| {
            if (field_value == .string) try fields.append(self.alloc, try self.alloc.dupe(u8, field_value.string));
        }
        if (object.get("fields")) |fields_value| {
            if (fields_value == .array) {
                for (fields_value.array.items) |item| {
                    if (item != .string) continue;
                    try fields.append(self.alloc, try self.alloc.dupe(u8, item.string));
                }
            }
        }
        return if (fields.items.len == 0) &.{} else try fields.toOwnedSlice(self.alloc);
    }

    fn queryBuilderEmbeddingIndexMetadata(
        self: *ApiHttpServer,
        index_name: []const u8,
        value: std.json.Value,
    ) !query_builder_agent.QueryBuilderEmbeddingIndex {
        const object = switch (value) {
            .object => |object| object,
            else => return .{ .name = try self.alloc.dupe(u8, index_name) },
        };
        const model = try self.queryBuilderEmbeddingIndexModel(object);
        errdefer if (model) |value_model| self.alloc.free(@constCast(value_model));
        return .{
            .name = try self.alloc.dupe(u8, index_name),
            .sparse = queryBuilderEmbeddingIndexIsSparse(object),
            .dimension = queryBuilderJsonInt(object.get("dimension")) orelse queryBuilderJsonInt(object.get("dims")),
            .model = model,
        };
    }

    fn queryBuilderEmbeddingIndexIsSparse(object: std.json.ObjectMap) bool {
        if (queryBuilderJsonString(object.get("type"))) |type_name| {
            if (std.mem.eql(u8, type_name, "sparse_vector")) return true;
            if (std.mem.eql(u8, type_name, "dense_vector")) return false;
        }
        return queryBuilderJsonBool(object.get("sparse")) orelse false;
    }

    fn queryBuilderEmbeddingIndexModel(self: *ApiHttpServer, object: std.json.ObjectMap) !?[]const u8 {
        if (queryBuilderJsonString(object.get("model"))) |model| return try self.alloc.dupe(u8, model);
        if (object.get("embedder")) |embedder_value| {
            if (embedder_value == .object) {
                if (queryBuilderJsonString(embedder_value.object.get("model"))) |model| return try self.alloc.dupe(u8, model);
            }
        }
        return null;
    }

    fn appendQueryBuilderGraphIndexMetadata(
        self: *ApiHttpServer,
        metadata: *std.ArrayListUnmanaged(query_builder_agent.QueryBuilderGraphIndex),
        index_name: []const u8,
        value: std.json.Value,
    ) !void {
        const name = try self.alloc.dupe(u8, index_name);
        errdefer self.alloc.free(name);
        const edge_types = try self.loadQueryBuilderGraphEdgeTypes(value);
        errdefer freeQueryBuilderGraphEdgeTypes(self.alloc, edge_types);
        const entry = query_builder_agent.QueryBuilderGraphIndex{
            .name = name,
            .edge_types = edge_types,
        };
        try metadata.append(self.alloc, entry);
    }

    fn loadQueryBuilderGraphEdgeTypes(self: *ApiHttpServer, value: std.json.Value) ![]const query_builder_agent.QueryBuilderGraphEdgeType {
        const object = switch (value) {
            .object => |object| object,
            else => return &.{},
        };
        const edge_types_value = object.get("edge_types") orelse object.get("edge_type_configs") orelse return &.{};
        if (edge_types_value != .array) return &.{};

        var edge_types = std.ArrayListUnmanaged(query_builder_agent.QueryBuilderGraphEdgeType).empty;
        defer edge_types.deinit(self.alloc);
        errdefer freeQueryBuilderGraphEdgeTypeItems(self.alloc, edge_types.items);

        for (edge_types_value.array.items) |item| {
            if (item != .object) continue;
            const name = queryBuilderJsonString(item.object.get("name")) orelse continue;
            const topology_source = queryBuilderJsonString(item.object.get("topology"));
            const edge_type = query_builder_agent.QueryBuilderGraphEdgeType{
                .name = try self.alloc.dupe(u8, name),
                .topology = if (topology_source) |topology| try self.alloc.dupe(u8, topology) else null,
            };
            errdefer freeQueryBuilderGraphEdgeTypeItem(self.alloc, edge_type);
            try edge_types.append(self.alloc, edge_type);
        }

        return if (edge_types.items.len == 0) &.{} else try edge_types.toOwnedSlice(self.alloc);
    }

    pub fn runtimeSchemaDebugAllowed(self: *ApiHttpServer, authenticated_identity: ?AuthenticatedIdentity) bool {
        if (!self.cfg.auth_enabled) return true;
        const identity = authenticated_identity orelse return false;
        return permissionsAllow(identity.permissions, .@"*", "*", .admin);
    }

    pub fn waitForTableVisibility(self: *ApiHttpServer, table_name: []const u8, expected: TableVisibility) !void {
        const handled = try self.source.waitTableLifecycle(table_name, expected);
        if (handled) return;

        const timeout_ns = 30 * std.time.ns_per_s;
        const poll_interval_ns = 50 * std.time.ns_per_ms;
        const start_ns = platform_time.monotonicNs();
        while (true) {
            const exists = try self.tableExists(table_name);
            if ((expected == .present and exists) or (expected == .absent and !exists)) return;
            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
            sleepNs(poll_interval_ns);
        }
    }

    pub fn waitForProjectedTablePresence(self: *ApiHttpServer, table_name: []const u8) !void {
        const timeout_ns = 10 * std.time.ns_per_s;
        const poll_interval_ns = 50 * std.time.ns_per_ms;
        const start_ns = platform_time.monotonicNs();
        while (true) {
            if (try self.tableExists(table_name)) return;
            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
            sleepNs(poll_interval_ns);
        }
    }

    pub fn waitForProjectedTableCreateReadiness(self: *ApiHttpServer, table_name: []const u8) !void {
        const timeout_ns = 30 * std.time.ns_per_s;
        const poll_interval_ns = 50 * std.time.ns_per_ms;
        const start_ns = platform_time.monotonicNs();
        while (true) {
            var maybe_snapshot = try self.source.adminSnapshot();
            if (maybe_snapshot) |*snapshot| {
                defer self.source.freeAdminSnapshot(snapshot);
                if (tableWriteQuorumReady(snapshot, table_name) or metadataOnlyTableReady(snapshot, table_name)) return;
            } else {
                return;
            }
            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
            sleepNs(poll_interval_ns);
        }
    }

    pub fn waitForProjectedTableWriteQuorum(self: *ApiHttpServer, table_name: []const u8) !void {
        const timeout_ns = 30 * std.time.ns_per_s;
        const poll_interval_ns = 50 * std.time.ns_per_ms;
        const start_ns = platform_time.monotonicNs();
        while (true) {
            var maybe_snapshot = try self.source.adminSnapshot();
            if (maybe_snapshot) |*snapshot| {
                defer self.source.freeAdminSnapshot(snapshot);
                if (tableWriteQuorumReady(snapshot, table_name)) return;
            } else {
                return;
            }
            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
            sleepNs(poll_interval_ns);
        }
    }

    fn tableWriteQuorumReady(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) bool {
        const table = tables_api.findTableByName(snapshot, table_name) orelse return false;
        var table_range_count: usize = 0;
        var placed_range_count: usize = 0;
        for (snapshot.ranges) |range| {
            if (range.table_id != table.table_id) continue;
            table_range_count += 1;
            const placement_count = countPlacementIntentsForGroup(snapshot.placement_intents, range.group_id);
            if (placement_count == 0) continue;
            placed_range_count += 1;
            const quorum = @divTrunc(placement_count, 2) + 1;
            if (countHealthyVoterReportsForGroup(snapshot.stores, range.group_id) < quorum) return false;
            if (!mergedGroupHasLeader(snapshot.merged_group_statuses, range.group_id)) return false;
        }
        return table_range_count > 0 and placed_range_count > 0;
    }

    fn metadataOnlyTableReady(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) bool {
        _ = tables_api.findTableByName(snapshot, table_name) orelse return false;
        return snapshot.stores.len == 0 and snapshot.placement_intents.len == 0;
    }

    fn mergedGroupHasLeader(statuses: []const metadata_reconciler.MergedGroupStatus, group_id: u64) bool {
        for (statuses) |status| {
            if (status.group_id == group_id and status.leader_known and status.leader_store_id != 0) return true;
        }
        return false;
    }

    fn countPlacementIntentsForGroup(intents: []const raft_reconciler.PlacementIntent, group_id: u64) usize {
        var count: usize = 0;
        for (intents) |intent| {
            if (intent.record.group_id == group_id) count += 1;
        }
        return count;
    }

    fn countHealthyVoterReportsForGroup(stores: []const metadata_table_manager.StoreRecord, group_id: u64) usize {
        var count: usize = 0;
        for (stores) |store| {
            if (!store.live) continue;
            if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
            var counted_store = false;
            for (store.group_statuses) |status| {
                if (status.group_id != group_id or !status.local_voter) continue;
                counted_store = true;
                break;
            }
            if (counted_store) count += 1;
        }
        return count;
    }

    pub fn waitForMetadataProjection(
        self: *ApiHttpServer,
        table_name: []const u8,
        expected_schema_json: ?[]const u8,
        expected_indexes_json: ?[]const u8,
    ) !void {
        const handled = self.source.waitTableProjection(table_name, expected_schema_json, expected_indexes_json) catch |err| switch (err) {
            error.TableVisibilityTimeout => {
                try self.waitForTableMetadata(table_name, expected_schema_json, expected_indexes_json);
                return;
            },
            else => return err,
        };
        if (handled) return;
        try self.waitForTableMetadata(table_name, expected_schema_json, expected_indexes_json);
    }

    fn hasLocalTableRuntime(self: *ApiHttpServer, table_name: []const u8) bool {
        const source = self.table_writes orelse return false;
        const maybe_statuses = source.localRuntimeStatuses(self.alloc, table_name) catch return false;
        if (maybe_statuses) |statuses| {
            var owned = statuses;
            defer owned.deinit(self.alloc);
            return owned.items.len > 0;
        }
        return false;
    }

    fn apiHttpServerCatalogAdminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return (try self.source.adminSnapshot()) orelse error.UnsupportedOperation;
    }

    fn apiHttpServerCatalogFreeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        self.source.freeAdminSnapshot(snapshot);
    }

    fn waitForTableMetadata(
        self: *ApiHttpServer,
        table_name: []const u8,
        expected_schema_json: ?[]const u8,
        expected_indexes_json: ?[]const u8,
    ) !void {
        const timeout_ns = 10 * std.time.ns_per_s;
        const poll_interval_ns = 50 * std.time.ns_per_ms;
        const start_ns = platform_time.monotonicNs();
        while (true) {
            var snapshot = (try self.source.adminSnapshot()) orelse {
                if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
                sleepNs(poll_interval_ns);
                continue;
            };
            defer self.source.freeAdminSnapshot(&snapshot);

            if (tables_api.findTableByName(&snapshot, table_name)) |table| {
                const schema_ok = if (expected_schema_json) |schema_json|
                    jsonDocumentsEqual(self.alloc, table.schema_json, schema_json) catch false
                else
                    true;
                const indexes_ok = if (expected_indexes_json) |indexes_json|
                    indexes_api.equivalentIndexConfigJson(self.alloc, table.indexes_json, indexes_json) catch false
                else
                    true;
                if (schema_ok and indexes_ok) return;

                if (platform_time.monotonicNs() -| start_ns >= timeout_ns) {
                    if (expected_indexes_json) |indexes_json| {
                        std.debug.print(
                            "waitForTableMetadata timeout table={s} observed_indexes={s} expected_indexes={s}\n",
                            .{ table_name, table.indexes_json, indexes_json },
                        );
                    }
                }
            }

            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
            sleepNs(poll_interval_ns);
        }
    }

    fn jsonDocumentsEqual(
        alloc: std.mem.Allocator,
        lhs_json: []const u8,
        rhs_json: []const u8,
    ) !bool {
        if (std.mem.eql(u8, lhs_json, rhs_json)) return true;

        var lhs = try parseJsonValueAlloc(alloc, lhs_json);
        defer lhs.deinit();
        var rhs = try parseJsonValueAlloc(alloc, rhs_json);
        defer rhs.deinit();
        return jsonValuesEqual(lhs.value, rhs.value);
    }

    fn jsonValuesEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
        return json_helpers.jsonValuesEqual(lhs, rhs);
    }

    fn backupOwnedTable(self: *ApiHttpServer, table_name: []const u8, backup_location: *backups_api.BackupLocation, backup_id: []const u8) !void {
        const table = (try self.loadOwnedTableRecord(table_name)) orelse return error.TableNotFound;
        defer metadata_table_manager.freeTable(self.alloc, table);
        if (table.read_schema_json.len > 0) return error.UnsupportedBackupMigrationState;

        const table_writes_source = self.table_writes orelse return error.UnsupportedOperation;
        const local_backup_root = switch (backup_location.*) {
            .file => |value| value,
            .remote => try createBackupStagingRoot(self.alloc, backup_id),
        };
        defer switch (backup_location.*) {
            .file => {},
            .remote => destroyBackupStagingRoot(self.alloc, local_backup_root),
        };

        const shards = (try table_writes_source.backupTable(self.alloc, table_name, .{
            .backup_root = local_backup_root,
            .backup_id = backup_id,
        })) orelse return error.TableNotFound;
        defer freeBackupShards(self.alloc, shards);

        if (switch (backup_location.*) {
            .remote => true,
            .file => false,
        }) {
            for (shards) |shard| {
                const snapshot_root = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ local_backup_root, shard.snapshot_path });
                defer self.alloc.free(snapshot_root);
                try backups_api.copyDirectoryToLocation(self.alloc, backup_location, backup_id, shard.group_id, snapshot_root);
            }
        }

        var manifest = try backups_api.createManifest(self.alloc, backup_id, &table, shards);
        defer manifest.deinit(self.alloc);
        try backups_api.writeManifestToLocation(self.alloc, backup_location, &manifest);
    }

    fn restoreOwnedTable(self: *ApiHttpServer, table_name: []const u8, backup_location: *backups_api.BackupLocation, backup_id: []const u8) !void {
        var manifest = backups_api.readManifestFromLocation(self.alloc, backup_location, backup_id) catch return error.InvalidBackupRequest;
        defer manifest.deinit(self.alloc);

        if (!std.mem.eql(u8, manifest.table_name, table_name)) return error.InvalidBackupRequest;
        if (manifest.read_schema_json.len > 0) return error.UnsupportedBackupMigrationState;
        if (try self.tableExists(table_name)) return error.TableAlreadyExists;

        var create_req = backups_api.createTableRequestFromManifest(self.alloc, &manifest) catch {
            return error.UnsupportedBackupFormat;
        };
        defer create_req.deinit(self.alloc);

        var created_metadata = false;
        self.source.createTable(self.alloc, table_name, create_req) catch |err| switch (err) {
            error.UnsupportedOperation => {},
            else => return err,
        };
        self.waitForMetadataProjection(table_name, manifest.schema_json, manifest.indexes_json) catch |err| switch (err) {
            error.TableVisibilityTimeout => return error.TableVisibilityTimeout,
            else => return err,
        };
        if (try self.tableExists(table_name)) created_metadata = true;
        errdefer if (created_metadata) self.source.dropTable(self.alloc, table_name) catch {};

        const table_writes_source = self.table_writes orelse return error.UnsupportedOperation;
        const local_backup_root = switch (backup_location.*) {
            .file => |value| value,
            .remote => try createBackupStagingRoot(self.alloc, backup_id),
        };
        defer switch (backup_location.*) {
            .file => {},
            .remote => destroyBackupStagingRoot(self.alloc, local_backup_root),
        };
        if (switch (backup_location.*) {
            .remote => true,
            .file => false,
        }) {
            for (manifest.shards) |shard| {
                const dest_root = try std.fmt.allocPrint(self.alloc, "{s}/{s}", .{ local_backup_root, shard.snapshot_path });
                defer self.alloc.free(dest_root);
                try backups_api.copyDirectoryFromLocation(self.alloc, backup_location, shard.snapshot_path, dest_root);
            }
        }

        const timeout_ns = 30 * std.time.ns_per_s;
        const poll_interval_ns = 50 * std.time.ns_per_ms;
        var restore_attempt: usize = 0;
        while (restore_attempt < 3) : (restore_attempt += 1) {
            const start_ns = platform_time.monotonicNs();
            while (true) {
                if ((table_writes_source.restoreTable(self.alloc, table_name, .{
                    .backup_root = local_backup_root,
                    .manifest = &manifest,
                }) catch |err| switch (err) {
                    error.UnsupportedOperation => return error.UnsupportedOperation,
                    error.UnsupportedBackupFormat => return error.UnsupportedBackupFormat,
                    else => {
                        std.log.err("restoreOwnedTable restoreTable failed table={s} backup_id={s} err={}", .{ table_name, backup_id, err });
                        return err;
                    },
                }) != null) break;

                if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
                sleepNs(poll_interval_ns);
            }

            // Wait until the read path can see the restored data. A null probe
            // means the catalog hasn't propagated the table yet; keep polling
            // rather than optimistically assuming success.
            const verify_deadline_ns = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
            while (true) {
                const storage_status = self.probeTableStorageStatus(table_name) catch null;
                if (storage_status) |status| {
                    if (!status.empty) break;
                }
                if (platform_time.monotonicNs() >= verify_deadline_ns) break;
                sleepNs(poll_interval_ns);
            }

            const storage_status = self.probeTableStorageStatus(table_name) catch null;
            if (storage_status != null and !storage_status.?.empty) return;
            std.log.info("restoreOwnedTable data not visible via read path table={s} backup_id={s} attempt={d}", .{
                table_name,
                backup_id,
                restore_attempt + 1,
            });
            if (restore_attempt + 1 >= 3) return error.TableVisibilityTimeout;
            sleepNs(500 * std.time.ns_per_ms);
        }
    }

    fn restoreOwnedTableWithRetry(
        self: *ApiHttpServer,
        table_name: []const u8,
        backup_location: *backups_api.BackupLocation,
        backup_id: []const u8,
    ) !void {
        var attempt: usize = 0;
        while (attempt < 3) : (attempt += 1) {
            self.restoreOwnedTable(table_name, backup_location, backup_id) catch |err| switch (err) {
                error.TableVisibilityTimeout => {
                    if (attempt + 1 >= 3) return err;
                    if ((self.tableExists(table_name) catch false)) {
                        self.waitForTableVisibility(table_name, .absent) catch {};
                    }
                    sleepNs(500 * std.time.ns_per_ms);
                    continue;
                },
                else => return err,
            };
            return;
        }
    }

    fn createBackupStagingRoot(alloc: std.mem.Allocator, backup_id: []const u8) ![]u8 {
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/api-backup-staging/{s}-{d}", .{
            backup_id,
            platform_time.monotonicNs(),
        });
        errdefer alloc.free(path);
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), path);
        return path;
    }

    fn destroyBackupStagingRoot(alloc: std.mem.Allocator, path: []const u8) void {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
        alloc.free(@constCast(path));
    }

    fn maybeRenewOwnedSessionLeases(self: *ApiHttpServer) !void {
        const ttl_ns = self.cfg.session_owner_lease_ttl_ns orelse return;
        _ = ttl_ns;
        const interval_ns = self.cfg.session_owner_lease_renew_interval_ns orelse return;
        const owner_node_id = self.localSessionNodeId();
        if (owner_node_id == 0) return;
        const now_ns = platform_time.monotonicNs();
        if (self.last_session_lease_renew_ns != 0 and now_ns -| self.last_session_lease_renew_ns < interval_ns) return;
        self.last_session_lease_renew_ns = now_ns;
        _ = self.txn_sessions.renewOwnedLeases(owner_node_id, now_ns) catch |err| switch (err) {
            error.SessionLeaseLost => return,
            else => return err,
        };
    }

    pub fn forwardSessionRequest(self: *ApiHttpServer, txn_id: db_mod.types.TxnId, req: http_common.HttpRequest) !?http_common.HttpResponse {
        const owner_node_id = (try self.txn_sessions.getOwnerNodeId(self.alloc, txn_id)) orelse transactions_api.sessionOwnerNodeId(txn_id);
        if (owner_node_id == 0) return null;
        const router = self.cfg.session_router orelse return null;
        const session_executor = self.cfg.session_executor orelse return null;
        if (owner_node_id == router.localNodeId()) return null;
        const base_uri = (try router.nodeBaseUri(self.alloc, owner_node_id)) orelse {
            if (try self.tryAdoptSession(txn_id)) return null;
            return null;
        };
        defer self.alloc.free(base_uri);
        const uri = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{ base_uri, req.uri });
        defer self.alloc.free(uri);
        return session_executor.execute(self.alloc, .{
            .method = req.method,
            .uri = uri,
            .headers = req.headers,
            .authorization = req.authorization,
            .content_type = req.content_type,
            .body = req.body,
        }) catch |err| {
            if (try self.tryAdoptSession(txn_id)) return null;
            return err;
        };
    }

    fn tryAdoptSession(self: *ApiHttpServer, txn_id: db_mod.types.TxnId) !bool {
        const local_node_id = self.localSessionNodeId();
        if (local_node_id == 0) return false;
        return try self.txn_sessions.adoptIfLeaseExpired(self.alloc, txn_id, local_node_id, null);
    }

    pub fn lookupStageReadSnapshot(
        self: *ApiHttpServer,
        table_name: []const u8,
        key: []const u8,
    ) !transactions_api.StageReadSnapshot {
        const source = self.table_reads orelse return .{
            .table_name = table_name,
            .key = key,
            .version = 0,
        };
        var lookup = (try source.lookup(self.alloc, table_name, key, .{}, .read_index)) orelse return .{
            .table_name = table_name,
            .key = key,
            .version = 0,
        };
        defer lookup.deinit(self.alloc);
        return .{
            .table_name = table_name,
            .key = key,
            .version = lookup.version,
            .document_json = try self.alloc.dupe(u8, lookup.json),
        };
    }

    fn currentVersionForConflict(self: *ApiHttpServer, table_name: []const u8, key: []const u8) !?u64 {
        const source = self.table_reads orelse return null;
        var lookup = (try source.lookup(self.alloc, table_name, key, .{}, .read_index)) orelse return 0;
        defer lookup.deinit(self.alloc);
        return lookup.version;
    }

    fn expectedVersionForConflict(req: transactions_api.OwnedTransactionCommitRequest, table_name: []const u8, key: []const u8) ?u64 {
        for (req.read_set) |item| {
            if (std.mem.eql(u8, item.table_name, table_name) and std.mem.eql(u8, item.key, key)) {
                return item.expected_version;
            }
        }
        for (req.tables) |table| {
            if (!std.mem.eql(u8, table.table_name, table_name)) continue;
            for (table.predicates.items) |predicate| {
                if (std.mem.eql(u8, predicate.key, key)) return predicate.expected_version;
            }
        }
        return null;
    }

    pub fn enrichCommitConflict(
        self: *ApiHttpServer,
        req: transactions_api.OwnedTransactionCommitRequest,
        conflict: distributed_txn.CommitConflict,
    ) !transactions_api.CommitConflict {
        const base = transactions_api.conflictFromOutcome(conflict);
        if (base.kind != .version_conflict) return base;
        var enriched = transactions_api.versionConflict(
            base.table_name,
            base.key,
            expectedVersionForConflict(req, base.table_name, base.key),
            try self.currentVersionForConflict(base.table_name, base.key),
        );
        enriched.group_id = base.group_id;
        enriched.phase = base.phase;
        return enriched;
    }

    fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        return try self.handle(req);
    }

    fn executeStreaming(ptr: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest, writer: http_common.StreamWriter) !bool {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        const uri_parts = splitTarget(req.uri);
        if (!std.mem.eql(u8, uri_parts.path, routes.Routes.a2a)) return false;
        if (!protocol_adapters.isA2aStreamingRequest(self.alloc, req)) return false;

        var authenticated_identity: ?AuthenticatedIdentity = null;
        defer if (authenticated_identity) |*identity| identity.deinit(self.alloc);
        if (self.requiresAuthentication(uri_parts.path)) {
            authenticated_identity = self.authenticateRequest(req.authorization) catch return false;
            const identity = authenticated_identity.?;
            if (requiresAdminPermission(uri_parts.path) and !permissionsAllow(identity.permissions, .@"*", "*", .admin)) return false;
            if (requiredPermissionForRequest(req.method, uri_parts.path)) |required| {
                if (!permissionsAllow(identity.permissions, required.resource_type, required.resource, required.permission_type)) return false;
            }
        }

        self.recordHandledRequest();
        return try protocol_adapters.handleA2aStreamingRequest(self, req, writer);
    }

    pub fn tableApi(self: *ApiHttpServer) public_table_http.TableApi {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute_table_batch = executePublicTableBatch,
                .execute_table_query_request = executePublicTableQueryRequest,
                .execute_table_query_view = executePublicTableQueryView,
                .execute_table_backup = executePublicTableBackup,
                .execute_table_restore = executePublicTableRestore,
                .execute_table_list_indexes = executePublicTableListIndexes,
                .execute_table_get_index = executePublicTableGetIndex,
                .execute_table_create_index = executePublicTableCreateIndex,
                .execute_table_delete_index = executePublicTableDeleteIndex,
            },
        };
    }

    pub fn clusterApi(self: *ApiHttpServer) cluster_api_http.ClusterApi {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute_cluster_backup_list = executePublicClusterBackupList,
                .execute_cluster_backup = executePublicClusterBackup,
                .execute_cluster_restore = executePublicClusterRestore,
            },
        };
    }

    fn executePublicTableBatch(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) public_table_http.TableApi.ExecuteBatchError!void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        const source = self.table_writes orelse return error.NotFound;
        _ = (source.batch(alloc, table_name, req) catch |err| switch (err) {
            error.InvalidBatchRequest => return error.InvalidBatchRequest,
            error.TableNotFound => return error.NotFound,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityUnavailable,
            error.EnrichmentRetryInProgress => return error.Backpressured,
            else => {
                std.log.err("public table batch failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        }) orelse return error.NotFound;
    }

    fn executePublicTableQueryRequest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
    ) public_table_http.TableApi.ExecuteQueryError![]u8 {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        const source = self.table_reads orelse return error.NotFound;
        return self.executePublicTableQueryDispatchWithReadinessRetry(alloc, source, table_name, body, row_filter_json, null) catch |err| switch (err) {
            error.InvalidQueryRequest => return error.InvalidQueryRequest,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityUnavailable,
            else => {
                std.log.err("public table query execution failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        };
    }

    fn executePublicTableQueryDispatch(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
    ) ![]u8 {
        return try self.executePublicTableQueryDispatchWithIdentity(alloc, source, table_name, body, row_filter_json, null);
    }

    fn executePublicTableQueryDispatchWithReadinessRetry(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
        authenticated_identity: ?AuthenticatedIdentity,
    ) ![]u8 {
        const retry_timeout_ns: u64 = if (self.table_writes != null) 5 * std.time.ns_per_s else 0;
        const retry_poll_ns = 50 * std.time.ns_per_ms;
        const start_ns = platform_time.monotonicNs();
        while (true) {
            return self.executePublicTableQueryDispatchWithIdentity(
                alloc,
                source,
                table_name,
                body,
                row_filter_json,
                authenticated_identity,
            ) catch |err| switch (err) {
                error.DocIdentityNamespaceMismatch => {
                    if (retry_timeout_ns > 0 and platform_time.monotonicNs() -| start_ns < retry_timeout_ns) {
                        sleepNs(retry_poll_ns);
                        continue;
                    }
                    return err;
                },
                else => return err,
            };
        }
    }

    fn executePublicTableQueryDispatchWithIdentity(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
        authenticated_identity: ?AuthenticatedIdentity,
    ) ![]u8 {
        if (try shouldDispatchPlainPublicSearch(alloc, body)) {
            var result = self.executePlainPublicTableQuery(
                alloc,
                source,
                table_name,
                body,
                row_filter_json,
            ) catch |err| switch (err) {
                error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
                error.TableNotFound => return error.TableNotFound,
                error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
                else => {
                    std.log.err("public table query execution failed table={s} err={}", .{ table_name, err });
                    return error.InternalFailure;
                },
            };
            defer result.deinit(alloc);
            return try alloc.dupe(u8, result.json);
        }

        var contract_req = metadata_openapi.server.parseQueryTableBody(alloc, body) catch return error.InvalidQueryRequest;
        defer contract_req.deinit();

        if (self.executeForeignPublicTableQueryIfAny(alloc, source, table_name, body, row_filter_json, authenticated_identity) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            else => {
                std.log.err("foreign public table query execution failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        }) |json| {
            return json;
        }

        const join_req = distributed_join.parseSupportedJoinRequestWithSecrets(alloc, body, self.cfg.secret_store) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
            else => {
                std.log.err("public table join parse failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        };
        if (join_req) |owned_join| {
            var parsed_join = owned_join;
            defer parsed_join.deinit(alloc);
            if (authenticated_identity) |identity| {
                try applyAuthenticatedIdentityToJoinRequest(alloc, identity, &parsed_join.join);
            }
            return distributed_join.executeSupportedJoinedPublicTableQueryRequest(self.joinContext(), &self.join_job_store, alloc, source, table_name, body, row_filter_json, parsed_join.join, parsed_join.foreign_sources);
        }

        var result = self.executePlainPublicTableQuery(
            alloc,
            source,
            table_name,
            body,
            row_filter_json,
        ) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
            error.TableNotFound => return error.NotFound,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            else => {
                std.log.err("public table query execution failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        };
        defer result.deinit(alloc);
        return try alloc.dupe(u8, result.json);
    }

    fn shouldDispatchPlainPublicSearch(alloc: std.mem.Allocator, body: []const u8) !bool {
        if (std.mem.indexOf(u8, body, "\"join\"") == null and
            std.mem.indexOf(u8, body, "\"foreign_sources\"") == null and
            (std.mem.indexOf(u8, body, "\"full_text_search\"") != null or
                std.mem.indexOf(u8, body, "\"embeddings\"") != null or
                std.mem.indexOf(u8, body, "\"filter_query\"") != null or
                std.mem.indexOf(u8, body, "\"exclusion_query\"") != null))
        {
            return true;
        }

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return false;
        defer parsed.deinit();
        const object = switch (parsed.value) {
            .object => |object| object,
            else => return false,
        };
        if (jsonObjectHasNonNullField(object, "join")) return false;
        if (jsonObjectHasNonNullField(object, "foreign_sources")) return false;
        return public_search_request.looksLikePublicSearchRequest(parsed.value) or
            jsonObjectHasNonNullField(object, "full_text_search") or
            jsonObjectHasNonNullField(object, "embeddings") or
            jsonObjectHasNonNullField(object, "filter_query") or
            jsonObjectHasNonNullField(object, "exclusion_query");
    }

    fn jsonObjectHasNonNullField(object: std.json.ObjectMap, key: []const u8) bool {
        const value = object.get(key) orelse return false;
        return value != .null;
    }

    fn executeForeignPublicTableQueryIfAny(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
        authenticated_identity: ?AuthenticatedIdentity,
    ) anyerror!?[]u8 {
        var parsed_request = metadata_openapi.server.parseQueryTableBody(alloc, body) catch return error.InvalidQueryRequest;
        defer parsed_request.deinit();
        const request = &parsed_request.value;
        if (row_filter_json) |value| {
            try injectRowFilterIntoOpenApiQueryRequest(alloc, request, value);
        }

        var foreign_sources = foreign_sources_api.postgresSourceMapFromMetadataOpenApiResolvedWithSecrets(alloc, request.foreign_sources, self.cfg.secret_store) catch |err| switch (err) {
            error.UnsupportedSourceKind => return error.UnsupportedQueryRequest,
            else => return err,
        };
        defer foreign_sources.deinit(alloc);

        const foreign_source = foreign_sources.get(table_name) orelse return null;
        try validateSupportedForeignPublicQueryRequest(request);

        if (request.join != null) {
            var parsed_join = (try distributed_join.parseSupportedJoinRequestWithSecrets(alloc, body, self.cfg.secret_store)) orelse return error.InvalidQueryRequest;
            defer parsed_join.deinit(alloc);
            if (authenticated_identity) |identity| {
                try applyAuthenticatedIdentityToJoinRequest(alloc, identity, &parsed_join.join);
            }
            return try self.executeSupportedJoinedForeignPublicTableQueryRequest(
                alloc,
                source,
                table_name,
                body,
                row_filter_json,
                foreign_source,
                parsed_join.join,
                parsed_join.foreign_sources,
            );
        }

        return try self.encodeForeignPublicTableQueryResponseAlloc(alloc, table_name, request.*, foreign_source);
    }

    fn encodeForeignPublicTableQueryResponseAlloc(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        request: anytype,
        foreign_source: foreign_mod.PostgresConfig,
    ) ![]u8 {
        const registry = try self.ensureForeignRegistry();
        const started_ns = platform_time.monotonicNs();
        const limit = try foreignQueryLimit(request.limit);
        const offset = try foreignQueryOffset(request.offset);
        const aggregations_json = if (request.aggregations) |aggregations|
            try std.json.Stringify.valueAlloc(alloc, aggregations, .{})
        else
            null;
        defer if (aggregations_json) |json| alloc.free(json);
        const aggregation_requests = if (aggregations_json) |json|
            try query_api.parseAggregationRequestsJson(alloc, json)
        else
            &.{};
        defer query_api.freeAggregationRequests(alloc, aggregation_requests);

        const raw_filter_query_json = if (request.filter_query) |query|
            try stringifyJsonValueAlloc(alloc, query)
        else
            null;
        defer if (raw_filter_query_json) |query| alloc.free(query);
        const filter_query_json = try foreign_sources_api.buildEffectiveFilterQueryJsonAlloc(
            alloc,
            foreign_source,
            raw_filter_query_json,
            request.filter_prefix,
        );
        defer if (filter_query_json) |query| alloc.free(query);

        const foreign_order_by = try cloneForeignSortFieldsAlloc(alloc, request.order_by);
        defer freeForeignSortFields(alloc, foreign_order_by);

        var params = try foreign_source.toQueryParams(alloc, .{
            .fields = if (request.fields) |fields| fields else &.{},
            .filter_query_json = filter_query_json,
            .limit = limit,
            .offset = offset,
            .order_by = foreign_order_by,
        });
        defer params.deinit(alloc);

        const source_config = try foreign_source.toSourceConfig(alloc);
        var foreign_query_source = try registry.create(alloc, source_config);
        defer foreign_query_source.deinit(alloc);

        var query_result = try foreign_query_source.query(alloc, params);
        defer query_result.deinit(alloc);
        const aggregation_results: []db_mod.aggregations.SearchAggregationResult = if (aggregation_requests.len > 0) blk: {
            var aggregate_params = try foreign_sources_api.buildPostgresAggregateParamsAlloc(
                alloc,
                foreign_source,
                aggregation_requests,
                filter_query_json,
            );
            defer aggregate_params.deinit(alloc);
            var aggregate_result = foreign_query_source.aggregate(alloc, aggregate_params) catch |err| switch (err) {
                error.UnsupportedAggregate => return error.UnsupportedQueryRequest,
                else => return err,
            };
            defer aggregate_result.deinit(alloc);
            break :blk try foreign_sources_api.foreignAggregateResultsToSearchResultsAlloc(alloc, aggregation_requests, aggregate_result);
        } else @constCast(@as([]const db_mod.aggregations.SearchAggregationResult, &.{}));

        const result_hits = if (request.count == true)
            try alloc.alloc(db_mod.types.SearchHit, 0)
        else
            try buildForeignSearchHitsAlloc(alloc, foreign_source, query_result.rows);
        var result: db_mod.types.SearchResult = .{
            .alloc = alloc,
            .hits = result_hits,
            .total_hits = @intCast(@min(query_result.total, std.math.maxInt(u32))),
        };
        defer result.deinit();

        const response_req: db_mod.types.SearchRequest = .{
            .count_only = request.count == true,
            .profile = request.profile == true,
            .aggregations_json = if (aggregations_json) |json| try alloc.dupe(u8, json) else &.{},
        };
        defer if (response_req.aggregations_json.len > 0) alloc.free(response_req.aggregations_json);
        var response_meta: query_api.QueryResponseMeta = .{
            .took_ms = @intCast(@divTrunc(platform_time.monotonicNs() - started_ns, std.time.ns_per_ms)),
            .aggregation_results = aggregation_results,
        };
        defer response_meta.deinit(alloc);

        var response = try query_api.encodeQueryResponses(alloc, table_name, .{
            .count_only = response_req.count_only,
            .profile = response_req.profile,
            .limit = @intCast(limit orelse 10),
            .offset = @intCast(offset),
            .aggregations_json = response_req.aggregations_json,
        }, response_meta, result);
        defer response.deinit(alloc);
        return try alloc.dupe(u8, response.json);
    }

    fn executeSupportedJoinedForeignPublicTableQueryRequest(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
        foreign_source: foreign_mod.PostgresConfig,
        join: SupportedJoinRequest,
        foreign_sources: foreign_mod.PostgresSourceMap,
    ) anyerror![]u8 {
        var contract_request = metadata_openapi.server.parseQueryTableBody(alloc, body) catch return error.InvalidQueryRequest;
        defer contract_request.deinit();
        const requested_left_fields = contract_request.value.fields orelse &.{};
        if (contract_request.value.count == true) return error.InvalidQueryRequest;
        const rewrite = try distributed_join.rewriteJoinedBaseQueryBodyAlloc(alloc, contract_request.value, join.left_field);
        const appended_left_field = rewrite.appended_left_field;
        const primary_body = rewrite.body;
        defer alloc.free(primary_body);

        var primary_request = try metadata_openapi.server.parseQueryTableBody(alloc, primary_body);
        defer primary_request.deinit();
        if (row_filter_json) |value| {
            try injectRowFilterIntoOpenApiQueryRequest(alloc, &primary_request.value, value);
        }
        const primary_json = try self.encodeForeignPublicTableQueryResponseAlloc(alloc, table_name, primary_request.value, foreign_source);
        defer alloc.free(primary_json);

        var owned_response = try parseOwnedJsonValueAlloc(alloc, primary_json);
        defer ApiHttpServer.deinitJsonValue(alloc, &owned_response);
        const hits_ptr = try distributed_join.queryHitsArrayPtr(&owned_response);
        if (hits_ptr.items.len == 0) return try alloc.dupe(u8, primary_json);

        const ctx = self.joinContext();
        const plan = try distributed_join.planSupportedJoinExecution(ctx, alloc, table_name, join, hits_ptr.items, foreign_sources);
        var right_result = try distributed_join.executeSupportedRightJoinQueryCoordinatorOnly(ctx, &self.join_job_store, alloc, source, join, hits_ptr.items, plan, foreign_sources);
        defer right_result.deinit(alloc);
        const stats = try distributed_join.applyJoinedRightHitsToResponse(
            alloc,
            &owned_response,
            hits_ptr.items,
            join,
            right_result.hits,
            requested_left_fields,
            appended_left_field,
        );
        try distributed_join.maybeAttachJoinProfile(alloc, &owned_response, stats, plan, right_result.strategy_used, right_result.distributed_execution, right_result.groups_queried);
        return try distributed_join.stringifyJsonValueAlloc(alloc, owned_response);
    }

    fn validateSupportedForeignPublicQueryRequest(request: anytype) !void {
        if (request.full_text_search != null) return error.UnsupportedQueryRequest;
        if (request.semantic_search != null) return error.UnsupportedQueryRequest;
        if (request.embedding_template != null) return error.UnsupportedQueryRequest;
        if (request.indexes != null) return error.UnsupportedQueryRequest;
        if (request.exclusion_query != null) return error.UnsupportedQueryRequest;
        if (request.embeddings != null) return error.UnsupportedQueryRequest;
        if (request.distance_under != null) return error.UnsupportedQueryRequest;
        if (request.distance_over != null) return error.UnsupportedQueryRequest;
        if (request.merge_config != null) return error.UnsupportedQueryRequest;
        if (request.reranker != null) return error.UnsupportedQueryRequest;
        if (request.analyses != null) return error.UnsupportedQueryRequest;
        if (request.graph_searches != null) return error.UnsupportedQueryRequest;
        if (request.expand_strategy != null) return error.UnsupportedQueryRequest;
        if (request.document_renderer != null) return error.UnsupportedQueryRequest;
        if (request.pruner != null) return error.UnsupportedQueryRequest;
        if (request.search_after != null) return error.UnsupportedQueryRequest;
        if (request.search_before != null) return error.UnsupportedQueryRequest;
    }

    fn foreignQueryLimit(limit: ?i64) !?usize {
        const raw = limit orelse 10;
        if (raw <= 0) return 10;
        return std.math.cast(usize, raw) orelse error.InvalidQueryRequest;
    }

    fn foreignQueryOffset(offset: ?i64) !usize {
        const raw = offset orelse 0;
        if (raw < 0) return error.InvalidQueryRequest;
        return std.math.cast(usize, raw) orelse error.InvalidQueryRequest;
    }

    fn cloneForeignSortFieldsAlloc(alloc: std.mem.Allocator, order_by: anytype) ![]foreign_mod.SortField {
        const fields = order_by orelse return &.{};
        if (fields.len == 0) return &.{};

        const out = try alloc.alloc(foreign_mod.SortField, fields.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*item| item.deinit(alloc);
            alloc.free(out);
        }
        for (fields, 0..) |field, idx| {
            out[idx] = .{
                .field = try alloc.dupe(u8, field.field),
                .desc = field.desc orelse false,
            };
            initialized += 1;
        }
        return out;
    }

    fn freeForeignSortFields(alloc: std.mem.Allocator, fields: []foreign_mod.SortField) void {
        for (fields) |*field| field.deinit(alloc);
        if (fields.len > 0) alloc.free(fields);
    }

    fn buildForeignSearchHitsAlloc(
        alloc: std.mem.Allocator,
        foreign_source: foreign_mod.PostgresConfig,
        rows: []const std.json.Value,
    ) ![]db_mod.types.SearchHit {
        if (rows.len == 0) return &.{};

        const hits = try alloc.alloc(db_mod.types.SearchHit, rows.len);
        var initialized: usize = 0;
        errdefer {
            for (hits[0..initialized]) |*hit| hit.deinit(alloc);
            alloc.free(hits);
        }

        for (rows, 0..) |row, idx| {
            if (row != .object) return error.InvalidQueryRequest;
            const id = if (try foreign_sources_api.deriveSearchIdAlloc(alloc, foreign_source, row)) |value|
                value
            else
                try std.fmt.allocPrint(alloc, "{d}", .{idx});
            errdefer alloc.free(id);

            const stored_data = try stringifyJsonValueAlloc(alloc, row);
            errdefer alloc.free(stored_data);

            hits[idx] = .{
                .id = id,
                .score = 1,
                .stored_data = stored_data,
            };
            initialized += 1;
        }
        return hits;
    }

    fn executePlainPublicTableQuery(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
    ) !query_api.QueryResponse {
        var semantic_resolver = SemanticStatusResolver{
            .source = self.source,
            .local_termite_provider = self.local_termite_provider,
            .remote_content = self.cfg.remote_content,
        };
        var query_req = query_api.parsePublicQueryRequest(alloc, semantic_resolver.iface(), table_name, body) catch |err| {
            std.log.warn("public table query parse failed table={s} err={}", .{ table_name, err });
            return error.InvalidQueryRequest;
        };
        defer query_req.deinit(alloc);
        self.maybeRouteQueryToReadSchema(table_name, &query_req.req) catch |err| switch (err) {
            error.TableNotFound => return error.TableNotFound,
            error.InvalidSchemaUpdateRequest, error.InvalidTableIndexMetadata => return error.InvalidQueryRequest,
            else => return err,
        };
        if (row_filter_json) |value| {
            injectRowFilterIntoSearchRequest(alloc, &query_req.req, value) catch return error.InvalidQueryRequest;
        }
        return (source.query(alloc, table_name, query_req.req, .read_index) catch |err| {
            std.log.warn("public table query read failed table={s} err={}", .{ table_name, err });
            return err;
        }) orelse error.TableNotFound;
    }

    fn stringifyJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
        return try json_helpers.stringifyJsonValueAlloc(alloc, value);
    }

    fn cloneJsonValue(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
        return try json_helpers.cloneJsonValue(alloc, value);
    }

    fn deinitJsonValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
        json_helpers.deinitJsonValue(alloc, value);
    }

    fn putOwnedJsonField(alloc: std.mem.Allocator, object: *std.json.ObjectMap, key: []const u8, value: std.json.Value) !void {
        try object.put(alloc, try alloc.dupe(u8, key), value);
    }

    fn buildOwnedSearchRequestFromQueryValue(
        self: *ApiHttpServer,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        query_value: std.json.Value,
    ) !query_api.OwnedQueryRequest {
        const query_body = try stringifyJsonValueAlloc(alloc, query_value);
        defer alloc.free(query_body);
        var semantic_resolver = SemanticStatusResolver{
            .source = self.source,
            .local_termite_provider = self.local_termite_provider,
            .remote_content = self.cfg.remote_content,
        };
        var owned = try query_api.parsePublicQueryRequest(alloc, semantic_resolver.iface(), table_name, query_body);
        errdefer owned.deinit(alloc);
        try self.maybeRouteQueryToReadSchema(table_name, &owned.req);
        return owned;
    }

    fn executePublicTableQueryView(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: public_table_http.TableApi.TableQueryView,
    ) public_table_http.TableApi.ExecuteQueryViewError![]u8 {
        return error.NotFound;
    }

    fn executePublicTableBackup(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        location: *backups_api.BackupLocation,
    ) public_table_http.TableApi.ExecuteBackupError!void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        self.backupOwnedTable(table_name, location, backup_id) catch |err| switch (err) {
            error.TableNotFound => return error.NotFound,
            error.UnsupportedOperation => return error.MethodNotAllowed,
            error.UnsupportedBackupMigrationState => return error.UnsupportedBackupMigrationState,
            error.UnsupportedMultiRangeTable => return error.UnsupportedMultiRangeTable,
            else => return error.InternalFailure,
        };
    }

    fn executePublicTableRestore(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        location_uri: []const u8,
        location: *backups_api.BackupLocation,
    ) public_table_http.TableApi.ExecuteRestoreError!void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        if (self.tableExists(table_name) catch return error.InternalFailure) return error.TableAlreadyExists;

        if (self.source.restoreTable(self.alloc, table_name, location_uri, backup_id) catch |err| switch (err) {
            error.UnsupportedOperation => false,
            error.InvalidBackupRequest => {
                if (self.tableExists(table_name) catch return error.InternalFailure) return error.TableAlreadyExists;
                return error.InvalidBackupRequest;
            },
            else => return mapExecuteRestoreError(err),
        }) return;

        self.restoreOwnedTableWithRetry(table_name, location, backup_id) catch |err| switch (err) {
            error.UnsupportedOperation => return error.MethodNotAllowed,
            error.InvalidBackupRequest => {
                if (self.tableExists(table_name) catch return error.InternalFailure) return error.TableAlreadyExists;
                return error.InvalidBackupRequest;
            },
            else => return mapExecuteRestoreError(err),
        };
    }

    fn mapExecuteRestoreError(err: anyerror) public_table_http.TableApi.ExecuteRestoreError {
        return switch (err) {
            error.TableAlreadyExists => error.TableAlreadyExists,
            error.UnsupportedBackupMigrationState => error.UnsupportedBackupMigrationState,
            error.UnsupportedBackupFormat => error.UnsupportedBackupFormat,
            error.InvalidBackupRequest => error.InvalidBackupRequest,
            else => error.InternalFailure,
        };
    }

    fn executePublicTableListIndexes(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) public_table_http.TableApi.ExecuteListIndexesError![]u8 {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        var snapshot = (self.statusAdminSnapshot() catch return error.InternalFailure) orelse return error.NotFound;
        defer self.source.freeAdminSnapshot(&snapshot);
        var local_statuses = self.localTableRuntimeStatusesWithSnapshot(table_name, &snapshot) catch return error.InternalFailure;
        defer if (local_statuses) |*status| status.deinit(self.alloc);
        return (indexes_api.encodeIndexList(
            alloc,
            &snapshot,
            table_name,
            if (local_statuses) |*status| status else null,
        ) catch return error.InternalFailure) orelse error.NotFound;
    }

    fn executePublicTableGetIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) public_table_http.TableApi.ExecuteGetIndexError![]u8 {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        var snapshot = (self.statusAdminSnapshot() catch return error.InternalFailure) orelse return error.NotFound;
        defer self.source.freeAdminSnapshot(&snapshot);
        const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.NotFound;
        var lookup = (indexes_api.lookupSingleIndexConfig(alloc, table.indexes_json, index_name) catch return error.InternalFailure) orelse return error.NotFound;
        defer lookup.deinit();
        var local_statuses = self.localTableRuntimeStatusesWithSnapshot(table_name, &snapshot) catch return error.InternalFailure;
        defer if (local_statuses) |*status| status.deinit(self.alloc);
        return indexes_api.encodeSingleIndexLookup(
            alloc,
            index_name,
            lookup.config,
            if (local_statuses) |*status| status else null,
        ) catch return error.InternalFailure;
    }

    fn executePublicTableCreateIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        body: []const u8,
    ) public_table_http.TableApi.ExecuteCreateIndexError!void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        const table_before = (self.loadOwnedTableRecord(table_name) catch return error.InternalFailure) orelse return error.NotFound;
        defer metadata_table_manager.freeTable(alloc, table_before);
        const index_json = table_contract.parseCreateIndexRequest(alloc, index_name, body) catch {
            return error.InvalidIndexRequest;
        };
        defer alloc.free(index_json);
        tables_api.validatePublicAlgebraicIndexJson(alloc, index_json) catch {
            return error.InvalidIndexRequest;
        };
        const expanded_index_json = tables_api.expandSchemaDerivedAlgebraicIndexAlloc(alloc, table_name, index_json, table_before.schema_json) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };
        defer alloc.free(expanded_index_json);
        const normalized_index_json = table_writes.normalizeManagedEmbeddingIndexDimensionJsonWithOptions(
            alloc,
            index_name,
            expanded_index_json,
            .{
                .local_termite_provider = self.local_termite_provider,
                .secret_store = self.cfg.secret_store,
                .remote_content = self.cfg.remote_content,
            },
        ) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };
        defer alloc.free(normalized_index_json);

        table_writes.validateIndexConfigWithOptions(alloc, index_name, normalized_index_json, .{
            .local_termite_provider = self.local_termite_provider,
            .secret_store = self.cfg.secret_store,
            .remote_content = self.cfg.remote_content,
        }) catch |err| switch (err) {
            error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };

        self.source.createIndex(alloc, table_name, index_name, normalized_index_json) catch |err| switch (err) {
            error.TableNotFound => return error.NotFound,
            error.UnsupportedOperation => return error.MethodNotAllowed,
            error.InvalidTableIndexMetadata, error.InvalidCreateIndexRequest, error.UnsupportedCreateTableRequest => return error.InvalidIndexRequest,
            else => {
                std.log.err("public create index metadata update failed table={s} index={s} err={}", .{ table_name, index_name, err });
                return error.InternalFailure;
            },
        };
        const expected_indexes_json = indexes_api.addIndexToTableIndexesJson(alloc, table_before.indexes_json, index_name, normalized_index_json) catch |err| switch (err) {
            error.InvalidTableIndexMetadata, error.InvalidCreateIndexRequest => return error.InvalidIndexRequest,
            else => return error.InternalFailure,
        };
        defer alloc.free(expected_indexes_json);
        self.waitForMetadataProjection(table_name, null, expected_indexes_json) catch |err| {
            std.log.err("public create index metadata projection wait failed table={s} index={s} err={}", .{ table_name, index_name, err });
            return error.InternalFailure;
        };
        if (self.table_writes) |table_writes_source| {
            _ = table_writes_source.createIndex(alloc, table_name, index_name, normalized_index_json) catch |err| switch (err) {
                error.InvalidCreateTableRequest, error.UnsupportedCreateTableRequest => return error.InvalidIndexRequest,
                else => {
                    std.log.err("public create index local apply failed table={s} index={s} err={}", .{ table_name, index_name, err });
                },
            };
        }
    }

    fn executePublicTableDeleteIndex(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) public_table_http.TableApi.ExecuteDeleteIndexError!void {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));
        const table_before = (self.loadOwnedTableRecord(table_name) catch return error.InternalFailure) orelse return error.NotFound;
        defer metadata_table_manager.freeTable(alloc, table_before);
        self.source.dropIndex(alloc, table_name, index_name) catch |err| switch (err) {
            error.TableNotFound, error.IndexNotFound => return error.NotFound,
            error.UnsupportedOperation => return error.MethodNotAllowed,
            else => return error.InternalFailure,
        };
        const expected_indexes_json = (indexes_api.removeIndexFromTableIndexesJson(alloc, table_before.indexes_json, index_name) catch return error.InternalFailure) orelse {
            return error.NotFound;
        };
        defer alloc.free(expected_indexes_json);
        self.waitForMetadataProjection(table_name, null, expected_indexes_json) catch {
            return error.InternalFailure;
        };
        if (self.table_writes) |table_writes_source| {
            _ = table_writes_source.dropIndex(alloc, table_name, index_name) catch |err| switch (err) {
                error.IndexNotFound => {},
                else => return error.InternalFailure,
            };
        }
    }

    fn executePublicClusterBackupList(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        location_uri: []const u8,
    ) cluster_api_http.ClusterApi.ExecuteListError![]u8 {
        _ = ptr;
        const infos = backups_api.listClusterBackupsFromLocation(alloc, location_uri) catch |err| {
            if (backups_api.backupLocationErrorMessage(err) != null) return error.UnsupportedBackupLocation;
            return error.InternalFailure;
        };
        defer backups_api.freeBackupInfos(alloc, infos);
        return backups_api.encodeBackupListResponse(alloc, infos) catch return error.InternalFailure;
    }

    fn executePublicClusterBackup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        req: backups_api.ClusterBackupRequest,
        location: *backups_api.BackupLocation,
    ) cluster_api_http.ClusterApi.ExecuteBackupError![]u8 {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));

        const owns_table_names = req.table_names == null;
        const table_names = if (req.table_names) |values|
            values
        else
            self.loadOwnedTableNames() catch return error.InternalFailure;
        defer if (owns_table_names) freeOwnedStrings(alloc, table_names);

        const statuses = alloc.alloc(backups_api.ClusterTableBackupStatus, table_names.len) catch return error.InternalFailure;
        defer alloc.free(statuses);
        var cluster_tables = std.ArrayListUnmanaged(backups_api.ClusterTableBackupEntry).empty;
        defer {
            for (cluster_tables.items) |*entry| entry.deinit(alloc);
            cluster_tables.deinit(alloc);
        }

        for (table_names, 0..) |table_name, i| {
            statuses[i] = .{ .name = table_name, .status = "failed", .@"error" = null };
            const table_backup_id = backups_api.clusterTableBackupId(alloc, req.backup_id, table_name) catch return error.InternalFailure;
            self.backupOwnedTable(table_name, location, table_backup_id) catch |err| {
                statuses[i].@"error" = switch (err) {
                    error.TableNotFound => "not found",
                    error.UnsupportedOperation => "method not allowed",
                    error.UnsupportedMultiRangeTable => "backup does not support multi-range tables",
                    error.UnsupportedBackupMigrationState => "backup does not support active schema migration",
                    else => "backup failed",
                };
                alloc.free(table_backup_id);
                continue;
            };
            statuses[i].status = "completed";
            const entry_name = alloc.dupe(u8, table_name) catch {
                alloc.free(table_backup_id);
                return error.InternalFailure;
            };
            errdefer alloc.free(entry_name);
            cluster_tables.append(alloc, .{
                .name = entry_name,
                .table_backup_id = table_backup_id,
            }) catch return error.InternalFailure;
        }

        var manifest = backups_api.createClusterManifest(alloc, req.backup_id, req.location, cluster_tables.items) catch return error.InternalFailure;
        defer manifest.deinit(alloc);
        backups_api.writeClusterManifestToLocation(alloc, location, &manifest) catch return error.InternalFailure;

        return backups_api.encodeClusterBackupResponse(alloc, req.backup_id, statuses) catch return error.InternalFailure;
    }

    fn executePublicClusterRestore(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        req: backups_api.ClusterRestoreRequest,
        location: *backups_api.BackupLocation,
        restore_mode: []const u8,
    ) cluster_api_http.ClusterApi.ExecuteRestoreError![]u8 {
        const self: *ApiHttpServer = @ptrCast(@alignCast(ptr));

        var manifest = backups_api.readClusterManifestFromLocation(alloc, location, req.backup_id) catch return error.InvalidRequest;
        defer manifest.deinit(alloc);

        const owns_table_names = req.table_names == null;
        const table_names = if (req.table_names) |values|
            values
        else
            cloneClusterManifestTableNames(alloc, &manifest) catch return error.InternalFailure;
        defer if (owns_table_names) freeOwnedStrings(alloc, table_names);

        if (std.mem.eql(u8, restore_mode, "fail_if_exists")) {
            for (table_names) |table_name| {
                if (self.tableExists(table_name) catch return error.InternalFailure) {
                    return error.TableAlreadyExists;
                }
            }
        }

        const statuses = alloc.alloc(backups_api.ClusterTableRestoreStatus, table_names.len) catch return error.InternalFailure;
        defer alloc.free(statuses);

        for (table_names, 0..) |table_name, i| {
            statuses[i] = .{ .name = table_name, .status = "failed", .@"error" = null };

            if (backups_api.findClusterTable(&manifest, table_name) == null) {
                statuses[i].@"error" = "backup does not include table";
                continue;
            }

            const exists = self.tableExists(table_name) catch return error.InternalFailure;
            if (std.mem.eql(u8, restore_mode, "skip_if_exists") and exists) {
                statuses[i].status = "skipped";
                continue;
            }
        }

        if (std.mem.eql(u8, restore_mode, "overwrite")) {
            for (table_names, 0..) |table_name, i| {
                if (statuses[i].@"error" != null or std.mem.eql(u8, statuses[i].status, "skipped")) continue;
                const exists = self.tableExists(table_name) catch return error.InternalFailure;
                if (!exists) continue;

                var local_drop_group_ids: ?[]u64 = null;
                defer if (local_drop_group_ids) |group_ids| alloc.free(group_ids);
                if (self.table_writes != null) {
                    if ((self.source.adminSnapshot() catch return error.InternalFailure)) |snapshot_value| {
                        var snapshot = snapshot_value;
                        defer self.source.freeAdminSnapshot(&snapshot);
                        local_drop_group_ids = tableGroupIdsFromSnapshot(alloc, &snapshot, table_name) catch return error.InternalFailure;
                    }
                }

                self.source.dropTable(alloc, table_name) catch |err| {
                    statuses[i].@"error" = switch (err) {
                        error.UnsupportedOperation => "method not allowed",
                        else => "failed to remove existing table",
                    };
                    continue;
                };
                if (self.table_writes) |write_source| {
                    const group_ids = local_drop_group_ids orelse &.{};
                    _ = write_source.dropTable(alloc, table_name, group_ids) catch |err| switch (err) {
                        error.TableNotFound => null,
                        else => {
                            statuses[i].@"error" = "failed to remove existing table";
                            continue;
                        },
                    };
                }
                self.waitForTableVisibility(table_name, .absent) catch {
                    statuses[i].@"error" = "failed to remove existing table";
                    continue;
                };
            }
        }

        const is_overwrite = std.mem.eql(u8, restore_mode, "overwrite");
        for (table_names, 0..) |table_name, i| {
            if (statuses[i].@"error" != null or std.mem.eql(u8, statuses[i].status, "skipped")) continue;

            const table_backup_id = backups_api.findClusterTable(&manifest, table_name).?.table_backup_id;

            // For overwrite, skip the metadata restore path and use the owned-table
            // restore which creates the table and copies data synchronously.
            if (!is_overwrite) {
                const restored_via_metadata = self.source.restoreTable(alloc, table_name, req.location, table_backup_id) catch |err| switch (err) {
                    error.UnsupportedOperation => false,
                    else => {
                        std.log.err("cluster restore failed table={s} backup_id={s} err={}", .{
                            table_name,
                            table_backup_id,
                            err,
                        });
                        statuses[i].@"error" = switch (err) {
                            error.UnsupportedBackupFormat => "restore does not support this backup layout",
                            error.UnsupportedBackupMigrationState => "restore does not support active schema migration",
                            error.TableAlreadyExists => "table already exists",
                            error.TableNotFound => "not found",
                            error.InvalidBackupRequest => "invalid restore request",
                            else => "restore failed",
                        };
                        continue;
                    },
                };
                if (restored_via_metadata) {
                    statuses[i].status = "triggered";
                    continue;
                }
            }

            self.restoreOwnedTableWithRetry(table_name, location, table_backup_id) catch |err| {
                std.log.err("cluster restore failed table={s} backup_id={s} err={}", .{
                    table_name,
                    table_backup_id,
                    err,
                });
                statuses[i].@"error" = switch (err) {
                    error.UnsupportedOperation => "method not allowed",
                    error.UnsupportedBackupFormat => "restore does not support this backup layout",
                    error.UnsupportedBackupMigrationState => "restore does not support active schema migration",
                    error.TableAlreadyExists => "table already exists",
                    error.TableNotFound => "not found",
                    error.InvalidBackupRequest => "invalid restore request",
                    else => "restore failed",
                };
                continue;
            };
            statuses[i].status = "triggered";
        }

        return backups_api.encodeClusterRestoreResponse(alloc, statuses) catch return error.InternalFailure;
    }

    pub fn handlePublicTableBatch(self: *ApiHttpServer, table_name: []const u8, body: []const u8) !http_common.HttpResponse {
        var resp = try public_table_http.handleTableBatch(self.alloc, table_name, body, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            201 => blk: {
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(metadata_openapi.BatchResponse, arena_impl.allocator(), resp.body);
                break :blk try jsonResponseWithStatus(self.alloc, 201, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicTableQuery(self: *ApiHttpServer, table_name: []const u8, body: []const u8, authenticated_identity: ?AuthenticatedIdentity) !http_common.HttpResponse {
        const row_filter_json = try resolveEffectiveRowFilterJson(self.alloc, authenticated_identity, table_name);
        defer if (row_filter_json) |value| self.alloc.free(value);

        const source = self.table_reads orelse return try textResponse(self.alloc, 404, "not found");
        const response_body = self.executePublicTableQueryDispatchWithReadinessRetry(
            self.alloc,
            source,
            table_name,
            body,
            row_filter_json,
            authenticated_identity,
        ) catch |err| switch (err) {
            error.InvalidQueryRequest => return try textResponse(self.alloc, 400, "invalid query request"),
            error.NotFound, error.TableNotFound => return try textResponse(self.alloc, 404, "not found"),
            error.DocIdentityNamespaceMismatch => return try textResponse(self.alloc, 503, "doc identity unavailable"),
            else => {
                std.log.err("public table query execution failed table={s} err={}", .{ table_name, err });
                return try textResponse(self.alloc, 500, "query failed");
            },
        };
        defer self.alloc.free(response_body);

        var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
        defer arena_impl.deinit();
        const parsed = try parseJsonResponseBody(metadata_openapi.QueryResponses, arena_impl.allocator(), response_body);
        return try jsonResponse(self.alloc, parsed);
    }

    pub fn handlePublicTableListIndexes(self: *ApiHttpServer, table_name: []const u8) !http_common.HttpResponse {
        var resp = try public_table_http.handleTableListIndexes(self.alloc, table_name, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => try jsonBodyResponseWithStatus(self.alloc, 200, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicTableGetIndex(self: *ApiHttpServer, table_name: []const u8, index_name: []const u8) !http_common.HttpResponse {
        var resp = try public_table_http.handleTableGetIndex(self.alloc, table_name, index_name, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => try jsonBodyResponseWithStatus(self.alloc, 200, resp.body),
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicTableCreateIndex(self: *ApiHttpServer, table_name: []const u8, index_name: []const u8, body: []const u8) !http_common.HttpResponse {
        var resp = try public_table_http.handleTableCreateIndex(self.alloc, table_name, index_name, body, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            201 => blk: {
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(struct {}, arena_impl.allocator(), resp.body);
                break :blk try jsonResponseWithStatus(self.alloc, 201, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicTableDeleteIndex(self: *ApiHttpServer, table_name: []const u8, index_name: []const u8) !http_common.HttpResponse {
        var resp = try public_table_http.handleTableDeleteIndex(self.alloc, table_name, index_name, self.tableApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            201 => blk: {
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(struct {}, arena_impl.allocator(), resp.body);
                break :blk try jsonResponseWithStatus(self.alloc, 201, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicTableBackup(self: *ApiHttpServer, table_name: []const u8, body: []const u8) !http_common.HttpResponse {
        var resp = try public_table_http.handleTableBackup(self.alloc, table_name, body, self.tableApi(), self.cfg.secret_store);
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            201 => blk: {
                const BackupSuccess = struct {
                    backup: []const u8,
                };
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(BackupSuccess, arena_impl.allocator(), resp.body);
                break :blk try jsonResponseWithStatus(self.alloc, 201, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicTableRestore(self: *ApiHttpServer, table_name: []const u8, body: []const u8) !http_common.HttpResponse {
        var resp = try public_table_http.handleTableRestore(self.alloc, table_name, body, self.tableApi(), self.cfg.secret_store);
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            202 => blk: {
                const RestoreTriggered = struct {
                    restore: []const u8,
                };
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(RestoreTriggered, arena_impl.allocator(), resp.body);
                break :blk try jsonResponseWithStatus(self.alloc, 202, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicClusterBackupList(self: *ApiHttpServer, location_uri: []const u8) !http_common.HttpResponse {
        var resp = try cluster_api_http.handleClusterBackupList(self.alloc, location_uri, self.clusterApi());
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => blk: {
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(metadata_openapi.BackupListResponse, arena_impl.allocator(), resp.body);
                break :blk try jsonResponse(self.alloc, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicClusterBackup(self: *ApiHttpServer, body: []const u8) !http_common.HttpResponse {
        var resp = try cluster_api_http.handleClusterBackup(self.alloc, body, self.clusterApi(), self.cfg.secret_store);
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            200 => blk: {
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(metadata_openapi.ClusterBackupResponse, arena_impl.allocator(), resp.body);
                break :blk try jsonResponse(self.alloc, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }

    pub fn handlePublicClusterRestore(self: *ApiHttpServer, body: []const u8) !http_common.HttpResponse {
        var resp = try cluster_api_http.handleClusterRestore(self.alloc, body, self.clusterApi(), self.cfg.secret_store);
        defer resp.deinit(self.alloc);
        return switch (resp.status) {
            202 => blk: {
                var arena_impl = std.heap.ArenaAllocator.init(self.alloc);
                defer arena_impl.deinit();
                const parsed = try parseJsonResponseBody(metadata_openapi.ClusterRestoreResponse, arena_impl.allocator(), resp.body);
                break :blk try jsonResponseWithStatus(self.alloc, 202, parsed);
            },
            else => try textResponse(self.alloc, resp.status, resp.body),
        };
    }
};

fn freeBackupShards(alloc: std.mem.Allocator, shards: []const backups_api.ShardSnapshot) void {
    for (shards) |shard| shard.deinit(alloc);
    alloc.free(@constCast(shards));
}

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn testMetadataServiceSourceWithoutLifecycle(svc: *metadata_service.MetadataService) StatusSource {
    const V = struct {
        fn status(ptr: *anyopaque) anyerror!metadata_api.MetadataStatus {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            return try service.status();
        }

        fn adminSnapshot(ptr: *anyopaque) anyerror!metadata_api.AdminSnapshot {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            return try service.adminSnapshot();
        }

        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            service.freeAdminSnapshot(snapshot);
        }

        fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) anyerror!void {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            return try createTableOnService(service, alloc, table_name, req);
        }

        fn dropTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) anyerror!void {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            return try dropTableOnService(service, alloc, table_name);
        }

        fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) anyerror!void {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            return try updateSchemaOnService(service, alloc, table_name, schema_json);
        }

        fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) anyerror!void {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            return try createIndexOnService(service, alloc, table_name, index_name, index_json);
        }

        fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) anyerror!void {
            const service: *metadata_service.MetadataService = @ptrCast(@alignCast(ptr));
            return try dropIndexOnService(service, alloc, table_name, index_name);
        }
    };

    return .{
        .ptr = svc,
        .vtable = &.{
            .status = V.status,
            .admin_snapshot = V.adminSnapshot,
            .free_admin_snapshot = V.freeAdminSnapshot,
            .create_table = V.createTable,
            .drop_table = V.dropTable,
            .update_schema = V.updateSchema,
            .create_index = V.createIndex,
            .drop_index = V.dropIndex,
        },
    };
}

pub fn freeOwnedStrings(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
    if (values.len > 0) alloc.free(@constCast(values));
}

fn freeOwnedStringItems(alloc: std.mem.Allocator, values: []const []const u8) void {
    for (values) |value| alloc.free(@constCast(value));
}

fn freeQueryBuilderIndexContext(alloc: std.mem.Allocator, context: QueryBuilderIndexContext) void {
    freeQueryBuilderFullTextIndexMetadata(alloc, context.full_text_index_metadata);
    freeQueryBuilderEmbeddingIndexMetadata(alloc, context.embedding_index_metadata);
    freeQueryBuilderGraphIndexMetadata(alloc, context.graph_index_metadata);
}

pub fn freeQueryBuilderTableContext(alloc: std.mem.Allocator, context: query_builder_agent.QueryBuilderTableContext) void {
    freeOwnedStrings(alloc, context.schema_fields);
    freeOwnedStrings(alloc, context.full_text_indexes);
    freeOwnedStrings(alloc, context.semantic_indexes);
    freeOwnedStrings(alloc, context.graph_indexes);
    freeQueryBuilderFullTextIndexMetadata(alloc, context.full_text_index_metadata);
    freeQueryBuilderEmbeddingIndexMetadata(alloc, context.embedding_index_metadata);
    freeQueryBuilderGraphIndexMetadata(alloc, context.graph_index_metadata);
}

fn freeQueryBuilderFullTextIndexMetadata(
    alloc: std.mem.Allocator,
    metadata: []const query_builder_agent.QueryBuilderFullTextIndex,
) void {
    freeQueryBuilderFullTextIndexMetadataItems(alloc, metadata);
    if (metadata.len > 0) alloc.free(@constCast(metadata));
}

fn freeQueryBuilderFullTextIndexMetadataItems(
    alloc: std.mem.Allocator,
    metadata: []const query_builder_agent.QueryBuilderFullTextIndex,
) void {
    for (metadata) |item| freeQueryBuilderFullTextIndexMetadataItem(alloc, item);
}

fn freeQueryBuilderFullTextIndexMetadataItem(
    alloc: std.mem.Allocator,
    metadata: query_builder_agent.QueryBuilderFullTextIndex,
) void {
    alloc.free(@constCast(metadata.name));
    freeOwnedStrings(alloc, metadata.fields);
}

fn freeQueryBuilderEmbeddingIndexMetadata(
    alloc: std.mem.Allocator,
    metadata: []const query_builder_agent.QueryBuilderEmbeddingIndex,
) void {
    freeQueryBuilderEmbeddingIndexMetadataItems(alloc, metadata);
    if (metadata.len > 0) alloc.free(@constCast(metadata));
}

fn freeQueryBuilderEmbeddingIndexMetadataItems(
    alloc: std.mem.Allocator,
    metadata: []const query_builder_agent.QueryBuilderEmbeddingIndex,
) void {
    for (metadata) |item| freeQueryBuilderEmbeddingIndexMetadataItem(alloc, item);
}

fn freeQueryBuilderEmbeddingIndexMetadataItem(
    alloc: std.mem.Allocator,
    metadata: query_builder_agent.QueryBuilderEmbeddingIndex,
) void {
    alloc.free(@constCast(metadata.name));
    if (metadata.model) |model| alloc.free(@constCast(model));
}

fn freeQueryBuilderGraphIndexMetadata(
    alloc: std.mem.Allocator,
    metadata: []const query_builder_agent.QueryBuilderGraphIndex,
) void {
    freeQueryBuilderGraphIndexMetadataItems(alloc, metadata);
    if (metadata.len > 0) alloc.free(@constCast(metadata));
}

fn freeQueryBuilderGraphIndexMetadataItems(
    alloc: std.mem.Allocator,
    metadata: []const query_builder_agent.QueryBuilderGraphIndex,
) void {
    for (metadata) |item| freeQueryBuilderGraphIndexMetadataItem(alloc, item);
}

fn freeQueryBuilderGraphIndexMetadataItem(
    alloc: std.mem.Allocator,
    metadata: query_builder_agent.QueryBuilderGraphIndex,
) void {
    alloc.free(@constCast(metadata.name));
    freeQueryBuilderGraphEdgeTypes(alloc, metadata.edge_types);
}

fn freeQueryBuilderGraphEdgeTypes(
    alloc: std.mem.Allocator,
    edge_types: []const query_builder_agent.QueryBuilderGraphEdgeType,
) void {
    freeQueryBuilderGraphEdgeTypeItems(alloc, edge_types);
    if (edge_types.len > 0) alloc.free(@constCast(edge_types));
}

fn freeQueryBuilderGraphEdgeTypeItems(
    alloc: std.mem.Allocator,
    edge_types: []const query_builder_agent.QueryBuilderGraphEdgeType,
) void {
    for (edge_types) |item| freeQueryBuilderGraphEdgeTypeItem(alloc, item);
}

fn freeQueryBuilderGraphEdgeTypeItem(
    alloc: std.mem.Allocator,
    edge_type: query_builder_agent.QueryBuilderGraphEdgeType,
) void {
    alloc.free(@constCast(edge_type.name));
    if (edge_type.topology) |topology| alloc.free(@constCast(topology));
}

fn queryBuilderJsonString(value: ?std.json.Value) ?[]const u8 {
    const unwrapped = value orelse return null;
    return switch (unwrapped) {
        .string => |string| string,
        else => null,
    };
}

fn queryBuilderJsonBool(value: ?std.json.Value) ?bool {
    const unwrapped = value orelse return null;
    return switch (unwrapped) {
        .bool => |boolean| boolean,
        else => null,
    };
}

fn queryBuilderJsonInt(value: ?std.json.Value) ?i64 {
    const unwrapped = value orelse return null;
    return switch (unwrapped) {
        .integer => |integer| integer,
        else => null,
    };
}

fn cloneClusterManifestTableNames(
    alloc: std.mem.Allocator,
    manifest: *const backups_api.ClusterBackupManifest,
) ![]const []const u8 {
    const names = try alloc.alloc([]const u8, manifest.tables.len);
    var initialized: usize = 0;
    errdefer {
        for (names[0..initialized]) |name| alloc.free(@constCast(name));
        alloc.free(names);
    }
    for (manifest.tables, 0..) |table, i| {
        names[i] = try alloc.dupe(u8, table.name);
        initialized += 1;
    }
    return names;
}

fn unauthorizedResponse(alloc: std.mem.Allocator) !http_common.HttpResponse {
    return .{
        .status = 401,
        .headers = try alloc.dupe(http_common.Header, &[_]http_common.Header{
            .{
                .name = try alloc.dupe(u8, "WWW-Authenticate"),
                .value = try alloc.dupe(u8, "Basic realm=\"antfly\""),
            },
        }),
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.fmt.allocPrint(alloc, "{{\"error\":{f}}}", .{std.json.fmt("unauthorized", .{})}),
    };
}

pub fn requiresAdminPermission(path: []const u8) bool {
    if (std.mem.eql(u8, path, routes.Routes.secrets) or std.mem.startsWith(u8, path, routes.Routes.secrets_prefix)) return true;
    if (std.mem.eql(u8, path, routes.Routes.backup) or std.mem.eql(u8, path, routes.Routes.restore) or std.mem.eql(u8, path, routes.Routes.backups)) return true;
    if (std.mem.eql(u8, path, routes.Routes.a2a) or std.mem.eql(u8, path, routes.Routes.agents_retrieval)) return true;
    if (std.mem.eql(u8, path, routes.Routes.users_me)) return false;
    if (std.mem.eql(u8, path, routes.Routes.auth_subjects) or std.mem.startsWith(u8, path, routes.Routes.auth_subjects_prefix)) return true;
    return std.mem.eql(u8, path, routes.Routes.users) or std.mem.startsWith(u8, path, routes.Routes.users_prefix);
}

pub const RequiredPermission = struct {
    resource_type: usermgr.ResourceType,
    resource: []const u8,
    permission_type: usermgr.PermissionType,
};

pub fn requiredPermissionForRequest(method: http_common.Method, path: []const u8) ?RequiredPermission {
    if (std.mem.eql(u8, path, routes.Routes.tables)) return switch (method) {
        .GET => .{
            .resource_type = .table,
            .resource = "*",
            .permission_type = .read,
        },
        .POST, .PUT, .DELETE => null,
    };
    if (routes.Routes.matchTableLookup(path)) |lookup| return .{
        .resource_type = .table,
        .resource = lookup.table_name,
        .permission_type = .read,
    };
    if (routes.Routes.matchTableQuery(path)) |query| return .{
        .resource_type = .table,
        .resource = query.table_name,
        .permission_type = .read,
    };
    if (routes.Routes.matchTablePath(path)) |table_path| {
        return .{
            .resource_type = .table,
            .resource = table_path.table_name,
            .permission_type = switch (method) {
                .GET => .read,
                .POST, .PUT, .DELETE => .admin,
            },
        };
    }
    if (routes.Routes.matchTableBatch(path)) |batch| return .{
        .resource_type = .table,
        .resource = batch.table_name,
        .permission_type = .write,
    };
    if (routes.Routes.matchTableMerge(path)) |merge| return .{
        .resource_type = .table,
        .resource = merge.table_name,
        .permission_type = .write,
    };
    if (routes.Routes.matchTableSchema(path)) |schema| return .{
        .resource_type = .table,
        .resource = schema.table_name,
        .permission_type = .admin,
    };
    if (routes.Routes.matchTableIndexes(path)) |indexes| return .{
        .resource_type = .table,
        .resource = indexes.table_name,
        .permission_type = switch (method) {
            .GET => .read,
            .POST => .admin,
            .PUT, .DELETE => return null,
        },
    };
    if (routes.Routes.matchTableIndex(path)) |index| return .{
        .resource_type = .table,
        .resource = index.table_name,
        .permission_type = switch (method) {
            .GET => .read,
            .DELETE => .admin,
            .POST => .admin,
            .PUT => return null,
        },
    };
    if (routes.Routes.matchTableBackup(path)) |table_backup| return .{
        .resource_type = .table,
        .resource = table_backup.table_name,
        .permission_type = .admin,
    };
    if (routes.Routes.matchTableRestore(path)) |table_restore| return .{
        .resource_type = .table,
        .resource = table_restore.table_name,
        .permission_type = .admin,
    };
    if (tableNameForGraphPath(path)) |table_name| return .{
        .resource_type = .table,
        .resource = table_name,
        .permission_type = .read,
    };
    return null;
}

fn tableNameForGraphPath(path: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, path, routes.Routes.tables_prefix)) return null;
    const rest = path[routes.Routes.tables_prefix.len..];
    const marker = std.mem.indexOf(u8, rest, "/query/graph/") orelse return null;
    if (marker == 0) return null;
    return rest[0..marker];
}

pub fn permissionsAllow(
    permissions: []const usermgr.Permission,
    resource_type: usermgr.ResourceType,
    resource: []const u8,
    permission_type: usermgr.PermissionType,
) bool {
    for (permissions) |permission| {
        const type_match = permission.resource_type == .@"*" or permission.resource_type == resource_type;
        const resource_match = std.mem.eql(u8, permission.resource, "*") or std.mem.eql(u8, permission.resource, resource);
        if (!type_match or !resource_match) continue;
        if (permission.type == .admin or permission.type == permission_type) return true;
    }
    return false;
}

fn applyAuthenticatedIdentityToJoinRequest(
    alloc: std.mem.Allocator,
    identity: AuthenticatedIdentity,
    join: *distributed_join.SupportedJoinRequest,
) !void {
    if (!permissionsAllow(identity.permissions, .table, join.right_table, .read)) return error.InvalidQueryRequest;

    const row_filter_json = try resolveEffectiveRowFilterJson(alloc, identity, join.right_table);
    defer if (row_filter_json) |value| alloc.free(value);
    if (row_filter_json) |value| {
        try distributed_join.applyRightTableRowFilterJson(alloc, join, value);
    }

    if (join.nested_join) |nested| {
        try applyAuthenticatedIdentityToJoinRequest(alloc, identity, nested);
    }
}

fn jsonResponseWithStatus(alloc: std.mem.Allocator, status: u16, value: anytype) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})}),
    };
}

fn jsonResponse(alloc: std.mem.Allocator, value: anytype) !http_common.HttpResponse {
    return try jsonResponseWithStatus(alloc, 200, value);
}

fn jsonBodyResponseWithStatus(
    alloc: std.mem.Allocator,
    status: u16,
    body: []const u8,
) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try alloc.dupe(u8, body),
    };
}

fn jsonResponseWithStatusOmitNullOptionals(
    alloc: std.mem.Allocator,
    status: u16,
    value: anytype,
) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false }),
    };
}

fn jsonResponseOmitNullOptionals(alloc: std.mem.Allocator, value: anytype) !http_common.HttpResponse {
    return try jsonResponseWithStatusOmitNullOptionals(alloc, 200, value);
}

fn parseJsonResponseBody(
    comptime T: type,
    alloc: std.mem.Allocator,
    body: []const u8,
) !T {
    return try std.json.parseFromSliceLeaky(T, alloc, body, .{
        .allocate = .alloc_always,
    });
}

fn jsonErrorResponse(alloc: std.mem.Allocator, status: u16, message: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "application/json"),
        .body = try std.fmt.allocPrint(alloc, "{{\"error\":{f}}}", .{std.json.fmt(message, .{})}),
    };
}

pub fn makeSecretEntry(listed: common_secrets.ListedSecret) metadata_openapi.SecretEntry {
    return .{
        .key = listed.key,
        .status = mapSecretStatus(listed.status),
        .env_var = listed.env_var,
        .created_at = listed.created_at,
        .updated_at = listed.updated_at,
    };
}

pub fn makeSecretList(alloc: std.mem.Allocator, listed: []const common_secrets.ListedSecret) !metadata_openapi.SecretList {
    const entries = try alloc.alloc(metadata_openapi.SecretEntry, listed.len);
    for (listed, 0..) |item, i| {
        entries[i] = makeSecretEntry(item);
    }
    return .{ .secrets = entries };
}

fn mapSecretStatus(status: common_secrets.SecretStatus) metadata_openapi.SecretStatus {
    return switch (status) {
        .configured_keystore => .configured_keystore,
        .configured_env => .configured_env,
        .configured_both => .configured_both,
    };
}

pub const OwnedCreateUserRequest = struct {
    username: []u8,
    password: []u8,
    initial_policies: []usermgr.Permission,
    metadata_json: []u8,

    pub fn deinit(self: *OwnedCreateUserRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.username);
        alloc.free(self.password);
        freePermissions(alloc, self.initial_policies);
        alloc.free(self.metadata_json);
        self.* = undefined;
    }
};

pub const OwnedCreateApiKeyRequest = struct {
    name: []u8,
    permissions: []usermgr.Permission,
    row_filter: []usermgr.RowFilterEntry,
    expires_at_ns: ?u64 = null,

    pub fn deinit(self: *OwnedCreateApiKeyRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.name);
        freePermissions(alloc, self.permissions);
        freeRowFilters(alloc, self.row_filter);
        self.* = undefined;
    }
};

pub fn parseCreateUserRequest(alloc: std.mem.Allocator, body: []const u8, path_username: []const u8) !OwnedCreateUserRequest {
    var parsed = try usermgr_openapi.server.parseCreateUserBody(alloc, body);
    defer parsed.deinit();
    if (parsed.value.password.len == 0) return error.InvalidCreateUserRequest;

    const username = if (parsed.value.username) |username_value| blk: {
        if (username_value.len == 0) return error.InvalidCreateUserRequest;
        if (!std.mem.eql(u8, username_value, path_username)) return error.InvalidCreateUserRequest;
        break :blk try alloc.dupe(u8, username_value);
    } else try alloc.dupe(u8, path_username);
    errdefer alloc.free(username);

    const password = try alloc.dupe(u8, parsed.value.password);
    errdefer alloc.free(password);

    const initial_policies = try clonePermissionsFromOpenApi(alloc, parsed.value.initial_policies);
    errdefer freePermissions(alloc, initial_policies);
    const metadata_json = try normalizeMetadataFromOpenApi(alloc, parsed.value.metadata);
    errdefer alloc.free(metadata_json);

    return .{
        .username = username,
        .password = password,
        .initial_policies = initial_policies,
        .metadata_json = metadata_json,
    };
}

pub fn parsePasswordUpdateRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var parsed = try usermgr_openapi.server.parseUpdateUserPasswordBody(alloc, body);
    defer parsed.deinit();
    if (parsed.value.new_password.len == 0) return error.InvalidPasswordUpdateRequest;
    return try alloc.dupe(u8, parsed.value.new_password);
}

pub fn parseCreateApiKeyRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedCreateApiKeyRequest {
    var parsed = try usermgr_openapi.server.parseCreateApiKeyBody(alloc, body);
    defer parsed.deinit();
    if (parsed.value.name.len == 0) return error.InvalidApiKeyRequest;
    const name = try alloc.dupe(u8, parsed.value.name);
    errdefer alloc.free(name);

    const permissions = try clonePermissionsFromOpenApi(alloc, parsed.value.permissions);
    errdefer freePermissions(alloc, permissions);

    const row_filter = try cloneRowFiltersFromOpenApi(alloc, parsed.value.row_filter);
    errdefer freeRowFilters(alloc, row_filter);

    const expires_at_ns = if (parsed.value.expires_in) |expires_in_value| blk: {
        if (expires_in_value.len == 0) break :blk null;
        break :blk nowNs() + try parseGoDurationNs(expires_in_value);
    } else null;

    return .{
        .name = name,
        .permissions = permissions,
        .row_filter = row_filter,
        .expires_at_ns = expires_at_ns,
    };
}

pub fn parsePermissionBody(alloc: std.mem.Allocator, body: []const u8) !usermgr.Permission {
    var parsed = try usermgr_openapi.server.parseAddPermissionToUserBody(alloc, body);
    defer parsed.deinit();
    return try permissionFromOpenApi(alloc, parsed.value);
}

pub fn parseRoleAssignmentBody(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var parsed = try usermgr_openapi.server.parseAddRoleToUserBody(alloc, body);
    defer parsed.deinit();
    if (parsed.value.role.len == 0) return error.InvalidRole;
    return try alloc.dupe(u8, parsed.value.role);
}

fn clonePermissionsFromOpenApi(alloc: std.mem.Allocator, permissions: ?[]const usermgr_openapi.Permission) ![]usermgr.Permission {
    const source = permissions orelse return try alloc.alloc(usermgr.Permission, 0);
    const out = try alloc.alloc(usermgr.Permission, source.len);
    errdefer alloc.free(out);
    var filled: usize = 0;
    errdefer for (out[0..filled]) |*permission| permission.deinit(alloc);
    for (source, 0..) |item, i| {
        out[i] = try permissionFromOpenApi(alloc, item);
        filled += 1;
    }
    return out;
}

fn normalizeMetadataFromOpenApi(
    alloc: std.mem.Allocator,
    metadata: ?std.json.ArrayHashMap(std.json.Value),
) ![]u8 {
    const value = metadata orelse return try alloc.dupe(u8, "{}");
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn permissionFromOpenApi(alloc: std.mem.Allocator, value: usermgr_openapi.Permission) !usermgr.Permission {
    return try usermgr.Permission.initOwned(
        alloc,
        resourceTypeFromOpenApi(value.resource_type),
        value.resource,
        permissionTypeFromOpenApi(value.type),
    );
}

fn cloneRowFiltersFromOpenApi(
    alloc: std.mem.Allocator,
    row_filter: ?std.json.ArrayHashMap(std.json.Value),
) ![]usermgr.RowFilterEntry {
    const source = row_filter orelse return try alloc.alloc(usermgr.RowFilterEntry, 0);
    const out = try alloc.alloc(usermgr.RowFilterEntry, source.map.count());
    errdefer alloc.free(out);
    var it = source.map.iterator();
    var filled: usize = 0;
    errdefer for (out[0..filled]) |*entry| entry.deinit(alloc);
    while (it.next()) |entry| {
        const filter_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        defer alloc.free(filter_json);
        try validateAuthRowFilterJson(alloc, filter_json);
        out[filled] = try usermgr.RowFilterEntry.initOwned(alloc, entry.key_ptr.*, filter_json);
        filled += 1;
    }
    return out;
}

fn parseRemovePermissionFromUserParams(query: []const u8) !usermgr_openapi.server.RemovePermissionFromUserParams {
    const resource = parseSimpleQueryParam(query, "resource") orelse return error.MissingResource;
    const resource_type = parseSimpleQueryParam(query, "resourceType") orelse return error.MissingResourceType;
    _ = usermgr.ResourceType.fromSlice(resource_type) catch return error.InvalidResourceType;
    return .{
        .resource = resource,
        .resource_type = resource_type,
    };
}

fn parseRemoveRoleFromUserParams(query: []const u8) !usermgr_openapi.server.RemoveRoleFromUserParams {
    const role = parseSimpleQueryParam(query, "role") orelse return error.MissingRole;
    if (role.len == 0) return error.MissingRole;
    return .{ .role = role };
}

fn parseListBackupsParams(query: []const u8) !metadata_openapi.server.ListBackupsParams {
    return .{
        .location = parseSimpleQueryParam(query, "location") orelse return error.MissingLocation,
    };
}

fn parseListTablesParams(query: []const u8) !metadata_openapi.server.ListTablesParams {
    return .{
        .prefix = parseSimpleQueryParam(query, "prefix"),
        .pattern = parseSimpleQueryParam(query, "pattern"),
    };
}

fn resourceTypeFromOpenApi(value: usermgr_openapi.ResourceType) usermgr.ResourceType {
    return switch (value) {
        .table => .table,
        .user => .user,
        .@"*" => .@"*",
    };
}

fn permissionTypeFromOpenApi(value: usermgr_openapi.PermissionType) usermgr.PermissionType {
    return switch (value) {
        .read => .read,
        .write => .write,
        .admin => .admin,
    };
}

fn resourceTypeToOpenApi(value: usermgr.ResourceType) usermgr_openapi.ResourceType {
    return switch (value) {
        .table => .table,
        .user => .user,
        .@"*" => .@"*",
    };
}

fn permissionTypeToOpenApi(value: usermgr.PermissionType) usermgr_openapi.PermissionType {
    return switch (value) {
        .read => .read,
        .write => .write,
        .admin => .admin,
    };
}

pub fn clonePermissionsToOpenApi(alloc: std.mem.Allocator, permissions: []const usermgr.Permission) ![]usermgr_openapi.Permission {
    const out = try alloc.alloc(usermgr_openapi.Permission, permissions.len);
    for (permissions, 0..) |permission, i| {
        out[i] = .{
            .resource = permission.resource,
            .resource_type = resourceTypeToOpenApi(permission.resource_type),
            .type = permissionTypeToOpenApi(permission.type),
        };
    }
    return out;
}

fn rowFilterMapToOpenApi(
    alloc: std.mem.Allocator,
    row_filters: []const usermgr.RowFilterEntry,
) !std.json.ArrayHashMap(std.json.Value) {
    var out = std.json.ArrayHashMap(std.json.Value){};
    for (row_filters) |entry| {
        const filter_value = try parseOwnedJsonValueAlloc(alloc, entry.filter);
        try out.map.put(alloc, entry.table, filter_value);
    }
    return out;
}

pub fn rowFilterEntryToOpenApi(
    alloc: std.mem.Allocator,
    entry: usermgr.RowFilterEntry,
) !usermgr_openapi.RowFilterEntry {
    const parsed_filter = try parseOwnedJsonObjectMapAlloc(alloc, entry.filter);
    return .{
        .table = entry.table,
        .filter = parsed_filter,
    };
}

pub fn apiKeyToOpenApi(
    alloc: std.mem.Allocator,
    api_key: usermgr.ApiKey,
) !usermgr_openapi.ApiKey {
    return .{
        .key_id = api_key.key_id,
        .name = api_key.name,
        .username = api_key.username,
        .permissions = if (api_key.permissions.len > 0) try clonePermissionsToOpenApi(alloc, api_key.permissions) else null,
        .row_filter = if (api_key.row_filter.len > 0) try rowFilterMapToOpenApi(alloc, api_key.row_filter) else null,
        .created_at = try formatTimestampOwned(alloc, api_key.created_at_ns),
        .expires_at = if (api_key.expires_at_ns) |value| try formatTimestampOwned(alloc, value) else null,
    };
}

pub fn createdApiKeyToOpenApi(
    alloc: std.mem.Allocator,
    created: usermgr.CreatedApiKey,
) !usermgr_openapi.ApiKeyWithSecret {
    const base = try apiKeyToOpenApi(alloc, created.key);
    return .{
        .key_id = base.key_id,
        .name = base.name,
        .username = base.username,
        .permissions = base.permissions,
        .row_filter = base.row_filter,
        .created_at = base.created_at,
        .expires_at = base.expires_at,
        .key_secret = created.key_secret,
        .encoded = created.encoded,
    };
}

fn parseGoDurationNs(raw: []const u8) !u64 {
    if (raw.len == 0) return error.InvalidDuration;
    var i: usize = 0;
    var total: u64 = 0;
    while (i < raw.len) {
        const start = i;
        while (i < raw.len and std.ascii.isDigit(raw[i])) : (i += 1) {}
        if (i == start) return error.InvalidDuration;
        const value = try std.fmt.parseUnsigned(u64, raw[start..i], 10);
        const unit: u64 = if (std.mem.startsWith(u8, raw[i..], "ns")) blk: {
            i += 2;
            break :blk @as(u64, 1);
        } else if (std.mem.startsWith(u8, raw[i..], "us")) blk: {
            i += 2;
            break :blk @as(u64, std.time.ns_per_us);
        } else if (std.mem.startsWith(u8, raw[i..], "ms")) blk: {
            i += 2;
            break :blk @as(u64, std.time.ns_per_ms);
        } else if (i < raw.len and raw[i] == 's') blk: {
            i += 1;
            break :blk @as(u64, std.time.ns_per_s);
        } else if (i < raw.len and raw[i] == 'm') blk: {
            i += 1;
            break :blk @as(u64, std.time.ns_per_min);
        } else if (i < raw.len and raw[i] == 'h') blk: {
            i += 1;
            break :blk @as(u64, std.time.ns_per_hour);
        } else if (i < raw.len and raw[i] == 'd') blk: {
            i += 1;
            break :blk @as(u64, 24 * std.time.ns_per_hour);
        } else return error.InvalidDuration;
        total += value * unit;
    }
    return total;
}

fn formatTimestampOwned(alloc: std.mem.Allocator, ns: u64) ![]u8 {
    const epoch_seconds = std.time.epoch.EpochSeconds{
        .secs = @divFloor(ns, std.time.ns_per_s),
    };
    const year_day = epoch_seconds.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();
    const day_seconds = epoch_seconds.getDaySeconds();
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        year_day.year,
        month_day.month.numeric(),
        month_day.day_index + 1,
        day_seconds.getHoursIntoDay(),
        day_seconds.getMinutesIntoHour(),
        day_seconds.getSecondsIntoMinute(),
    });
}

fn nowNs() u64 {
    return platform_time.realtimeNs();
}

pub fn freePermissions(alloc: std.mem.Allocator, permissions: []const usermgr.Permission) void {
    for (permissions) |permission| {
        var owned = permission;
        owned.deinit(alloc);
    }
    alloc.free(@constCast(permissions));
}

pub fn freeRowFilters(alloc: std.mem.Allocator, row_filters: []const usermgr.RowFilterEntry) void {
    for (row_filters) |entry| {
        var owned = entry;
        owned.deinit(alloc);
    }
    alloc.free(@constCast(row_filters));
}

pub fn freeAuthSubjects(alloc: std.mem.Allocator, subjects: []const usermgr.AuthSubjectEntry) void {
    for (subjects) |entry| {
        var owned = entry;
        owned.deinit(alloc);
    }
    alloc.free(@constCast(subjects));
}

pub const AuthSubjectResponse = struct {
    subject: []const u8,
    kind: []const u8,
};

pub fn authSubjectsToResponse(
    alloc: std.mem.Allocator,
    subjects: []const usermgr.AuthSubjectEntry,
) ![]const AuthSubjectResponse {
    const out = try alloc.alloc(AuthSubjectResponse, subjects.len);
    for (subjects, 0..) |entry, i| {
        out[i] = .{
            .subject = entry.subject,
            .kind = entry.kind.slice(),
        };
    }
    return out;
}

pub fn effectiveRowFilterJson(identity: ?AuthenticatedIdentity, table_name: []const u8) ?[]const u8 {
    const row_filters = if (identity) |value| value.row_filter else return null;
    for (row_filters) |entry| {
        if (std.mem.eql(u8, entry.table, table_name)) {
            if (std.mem.eql(u8, entry.filter, "null")) return null;
            return entry.filter;
        }
    }
    for (row_filters) |entry| {
        if (std.mem.eql(u8, entry.table, "*")) {
            if (std.mem.eql(u8, entry.filter, "null")) return null;
            return entry.filter;
        }
    }
    return null;
}

pub fn resolveEffectiveRowFilterJson(
    alloc: std.mem.Allocator,
    identity: ?AuthenticatedIdentity,
    table_name: []const u8,
) !?[]u8 {
    const raw = effectiveRowFilterJson(identity, table_name) orelse return null;
    const active_identity = identity orelse return try alloc.dupe(u8, raw);
    return try resolveAuthRowFilterJson(alloc, active_identity, raw);
}

pub fn resolveAuthRowFilterJson(
    alloc: std.mem.Allocator,
    identity: AuthenticatedIdentity,
    filter_json: []const u8,
) ![]u8 {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();

    var resolved = try resolveAuthRowFilterValue(alloc, identity, parsed.value);
    defer json_helpers.deinitJsonValue(alloc, &resolved);
    return try json_helpers.stringifyJsonValueAlloc(alloc, resolved);
}

pub fn validateAuthRowFilterJson(
    alloc: std.mem.Allocator,
    filter_json: []const u8,
) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, filter_json, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    try validateAuthRowFilterValue(parsed.value);
}

fn validateAuthRowFilterValue(value: std.json.Value) !void {
    switch (value) {
        .object => |object| {
            if (object.get("$auth")) |auth_ref| {
                if (object.count() != 1) return error.InvalidQueryRequest;
                const path = if (auth_ref == .string) auth_ref.string else return error.InvalidQueryRequest;
                if (!isSupportedAuthPath(path)) return error.InvalidQueryRequest;
                return;
            }

            var it = object.iterator();
            while (it.next()) |entry| {
                try validateAuthRowFilterValue(entry.value_ptr.*);
            }
        },
        .array => |array| {
            for (array.items) |item| {
                try validateAuthRowFilterValue(item);
            }
        },
        else => {},
    }
}

fn resolveAuthRowFilterValue(
    alloc: std.mem.Allocator,
    identity: AuthenticatedIdentity,
    value: std.json.Value,
) !std.json.Value {
    return switch (value) {
        .object => |object| blk: {
            if (object.get("$auth")) |auth_ref| {
                if (object.count() != 1) return error.InvalidQueryRequest;
                const path = if (auth_ref == .string) auth_ref.string else return error.InvalidQueryRequest;
                break :blk try authContextJsonValue(alloc, identity, path);
            }

            var out = std.json.ObjectMap.empty;
            errdefer {
                var it = out.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    json_helpers.deinitJsonValue(alloc, entry.value_ptr);
                }
                out.deinit(alloc);
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                const key = try alloc.dupe(u8, entry.key_ptr.*);
                var key_transferred = false;
                errdefer if (!key_transferred) alloc.free(key);
                var resolved = try resolveAuthRowFilterValue(alloc, identity, entry.value_ptr.*);
                var value_transferred = false;
                errdefer if (!value_transferred) json_helpers.deinitJsonValue(alloc, &resolved);
                try out.put(alloc, key, resolved);
                key_transferred = true;
                value_transferred = true;
            }
            break :blk .{ .object = out };
        },
        .array => |array| blk: {
            var out = std.json.Array.init(alloc);
            errdefer {
                for (out.items) |*item| json_helpers.deinitJsonValue(alloc, item);
                out.deinit();
            }
            for (array.items) |item| {
                var resolved = try resolveAuthRowFilterValue(alloc, identity, item);
                var value_transferred = false;
                errdefer if (!value_transferred) json_helpers.deinitJsonValue(alloc, &resolved);
                try out.append(resolved);
                value_transferred = true;
            }
            break :blk .{ .array = out };
        },
        else => try json_helpers.cloneJsonValue(alloc, value),
    };
}

fn authContextJsonValue(
    alloc: std.mem.Allocator,
    identity: AuthenticatedIdentity,
    path: []const u8,
) !std.json.Value {
    if (isSupportedAuthPath(path)) {
        if (std.mem.eql(u8, path, "username")) {
            return .{ .string = try alloc.dupe(u8, identity.username) };
        }
        if (std.mem.eql(u8, path, "roles")) {
            return try rolesAuthJsonValue(alloc, identity.roles);
        }
        return try metadataAuthJsonValue(alloc, identity.metadata_json, path["metadata.".len..]);
    }
    return error.InvalidQueryRequest;
}

fn isSupportedAuthPath(path: []const u8) bool {
    if (std.mem.eql(u8, path, "username")) return true;
    if (std.mem.eql(u8, path, "roles")) return true;
    if (!std.mem.startsWith(u8, path, "metadata.")) return false;
    var segments = std.mem.splitScalar(u8, path["metadata.".len..], '.');
    var seen = false;
    while (segments.next()) |segment| {
        if (segment.len == 0) return false;
        seen = true;
    }
    return seen;
}

fn rolesAuthJsonValue(
    alloc: std.mem.Allocator,
    roles: []const []const u8,
) !std.json.Value {
    var out = std.json.Array.init(alloc);
    errdefer {
        for (out.items) |*item| json_helpers.deinitJsonValue(alloc, item);
        out.deinit();
    }
    for (roles) |role| {
        try out.append(.{ .string = try alloc.dupe(u8, role) });
    }
    return .{ .array = out };
}

fn metadataAuthJsonValue(
    alloc: std.mem.Allocator,
    metadata_json: []const u8,
    path: []const u8,
) !std.json.Value {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, if (metadata_json.len > 0) metadata_json else "{}", .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();

    var current = parsed.value;
    var segments = std.mem.splitScalar(u8, path, '.');
    while (segments.next()) |segment| {
        if (segment.len == 0 or current != .object) return error.InvalidQueryRequest;
        current = current.object.get(segment) orelse return error.InvalidQueryRequest;
    }
    return try json_helpers.cloneJsonValue(alloc, current);
}

test "auth row filter resolver expands username references" {
    const alloc = std.testing.allocator;
    var identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "alice"),
    };
    defer identity.deinit(alloc);

    const resolved = try resolveAuthRowFilterJson(
        alloc,
        identity,
        "{\"term\":{\"owner\":{\"$auth\":\"username\"}}}",
    );
    defer alloc.free(resolved);

    try std.testing.expectEqualStrings("{\"term\":{\"owner\":\"alice\"}}", resolved);
}

test "auth row filter resolver expands metadata references" {
    const alloc = std.testing.allocator;
    var identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "alice"),
        .metadata_json = try alloc.dupe(u8, "{\"tenant_id\":\"acme\",\"limits\":{\"tier\":\"gold\"}}"),
    };
    defer identity.deinit(alloc);

    const resolved = try resolveAuthRowFilterJson(
        alloc,
        identity,
        "{\"conjuncts\":[{\"term\":{\"tenant_id\":{\"$auth\":\"metadata.tenant_id\"}}},{\"term\":{\"tier\":{\"$auth\":\"metadata.limits.tier\"}}}]}",
    );
    defer alloc.free(resolved);

    try std.testing.expect(std.mem.indexOf(u8, resolved, "\"tenant_id\":\"acme\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resolved, "\"tier\":\"gold\"") != null);
}

test "auth row filter resolver expands role references" {
    const alloc = std.testing.allocator;
    const roles = try alloc.alloc([]u8, 2);
    roles[0] = try alloc.dupe(u8, "role:tenant_reader");
    roles[1] = try alloc.dupe(u8, "group:eng");
    var identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "alice"),
        .roles = roles,
    };
    defer identity.deinit(alloc);

    const resolved = try resolveAuthRowFilterJson(
        alloc,
        identity,
        "{\"terms\":{\"acl.roles\":{\"$auth\":\"roles\"}}}",
    );
    defer alloc.free(resolved);

    try std.testing.expect(std.mem.indexOf(u8, resolved, "\"role:tenant_reader\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resolved, "\"group:eng\"") != null);
}

test "auth row filter validator accepts username references" {
    try validateAuthRowFilterJson(
        std.testing.allocator,
        "{\"conjuncts\":[{\"term\":{\"owner\":{\"$auth\":\"username\"}}},{\"term\":{\"tenant_id\":{\"$auth\":\"metadata.tenant_id\"}}},{\"terms\":{\"acl.roles\":{\"$auth\":\"roles\"}}}]}",
    );
}

test "auth row filter resolver rejects unsupported auth paths" {
    const alloc = std.testing.allocator;
    var identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "alice"),
    };
    defer identity.deinit(alloc);

    try std.testing.expectError(
        error.InvalidQueryRequest,
        validateAuthRowFilterJson(alloc, "{\"term\":{\"owner\":{\"$auth\":\"_user.username\"}}}"),
    );
    try std.testing.expectError(
        error.InvalidQueryRequest,
        validateAuthRowFilterJson(alloc, "{\"term\":{\"owner\":{\"$auth\":\"metadata.\"}}}"),
    );
    try std.testing.expectError(
        error.InvalidQueryRequest,
        resolveAuthRowFilterJson(alloc, identity, "{\"term\":{\"owner\":{\"$auth\":\"_user.username\"}}}"),
    );
}

test "auth row filter validator rejects malformed auth node" {
    try std.testing.expectError(
        error.InvalidQueryRequest,
        validateAuthRowFilterJson(std.testing.allocator, "{\"$auth\":\"username\",\"extra\":true}"),
    );
}

test "effective resolved row filter prefers table filter before wildcard" {
    const alloc = std.testing.allocator;
    var row_filters = [_]usermgr.RowFilterEntry{
        try usermgr.RowFilterEntry.initOwned(alloc, "*", "{\"term\":{\"visibility\":\"public\"}}"),
        try usermgr.RowFilterEntry.initOwned(alloc, "docs", "{\"term\":{\"owner\":{\"$auth\":\"username\"}}}"),
    };
    defer {
        for (&row_filters) |*entry| entry.deinit(alloc);
    }
    const identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "bob"),
        .row_filter = row_filters[0..],
    };
    defer alloc.free(identity.username);

    const resolved = (try resolveEffectiveRowFilterJson(alloc, identity, "docs")) orelse return error.TestExpectedEqual;
    defer alloc.free(resolved);

    try std.testing.expectEqualStrings("{\"term\":{\"owner\":\"bob\"}}", resolved);
}

test "join auth applies read permission and row filters to joined tables" {
    const alloc = std.testing.allocator;
    var permissions = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .table, "customers", .read),
        try usermgr.Permission.initOwned(alloc, .table, "addresses", .read),
    };
    defer {
        for (&permissions) |*permission| permission.deinit(alloc);
    }
    var row_filters = [_]usermgr.RowFilterEntry{
        try usermgr.RowFilterEntry.initOwned(alloc, "customers", "{\"term\":{\"tenant_id\":{\"$auth\":\"metadata.tenant_id\"}}}"),
        try usermgr.RowFilterEntry.initOwned(alloc, "addresses", "{\"term\":{\"region\":\"us\"}}"),
    };
    defer {
        for (&row_filters) |*entry| entry.deinit(alloc);
    }
    const nested = try alloc.create(distributed_join.SupportedJoinRequest);
    nested.* = .{
        .right_table = try alloc.dupe(u8, "addresses"),
        .left_field = try alloc.dupe(u8, "address_id"),
        .right_field = try alloc.dupe(u8, "id"),
    };
    var join = distributed_join.SupportedJoinRequest{
        .right_table = try alloc.dupe(u8, "customers"),
        .left_field = try alloc.dupe(u8, "customer_id"),
        .right_field = try alloc.dupe(u8, "id"),
        .nested_join = nested,
    };
    defer join.deinit(alloc);
    const identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "alice"),
        .permissions = permissions[0..],
        .row_filter = row_filters[0..],
        .metadata_json = try alloc.dupe(u8, "{\"tenant_id\":\"acme\"}"),
    };
    defer {
        alloc.free(identity.username);
        alloc.free(identity.metadata_json);
    }

    try applyAuthenticatedIdentityToJoinRequest(alloc, identity, &join);

    const customer_filter = join.right_filters.?.filter_query orelse return error.TestExpectedEqual;
    const customer_json = try distributed_join.stringifyJsonValueAlloc(alloc, customer_filter);
    defer alloc.free(customer_json);
    try std.testing.expect(std.mem.indexOf(u8, customer_json, "\"tenant_id\":\"acme\"") != null);

    const nested_filter = join.nested_join.?.right_filters.?.filter_query orelse return error.TestExpectedEqual;
    const nested_json = try distributed_join.stringifyJsonValueAlloc(alloc, nested_filter);
    defer alloc.free(nested_json);
    try std.testing.expect(std.mem.indexOf(u8, nested_json, "\"region\":\"us\"") != null);
}

test "join auth rejects joined table without read permission" {
    const alloc = std.testing.allocator;
    var join = distributed_join.SupportedJoinRequest{
        .right_table = try alloc.dupe(u8, "customers"),
        .left_field = try alloc.dupe(u8, "customer_id"),
        .right_field = try alloc.dupe(u8, "id"),
    };
    defer join.deinit(alloc);
    const identity = AuthenticatedIdentity{
        .username = try alloc.dupe(u8, "alice"),
    };
    defer alloc.free(identity.username);

    try std.testing.expectError(error.InvalidQueryRequest, applyAuthenticatedIdentityToJoinRequest(alloc, identity, &join));
}

fn injectRowFilterIntoSearchRequest(
    alloc: std.mem.Allocator,
    req: *db_mod.types.SearchRequest,
    row_filter_json: []const u8,
) !void {
    if (req.filter_query_json.len == 0) {
        req.filter_query_json = try alloc.dupe(u8, row_filter_json);
        return;
    }
    const conjunction = try std.fmt.allocPrint(
        alloc,
        "{{\"conjuncts\":[{s},{s}]}}",
        .{ req.filter_query_json, row_filter_json },
    );
    alloc.free(req.filter_query_json);
    req.filter_query_json = conjunction;
}

fn injectRowFilterIntoOpenApiQueryRequest(
    alloc: std.mem.Allocator,
    req: anytype,
    row_filter_json: []const u8,
) !void {
    var combined = try distributed_join.combineFilterQueryWithRowFilterJson(alloc, req.filter_query, row_filter_json);
    errdefer json_helpers.deinitJsonValue(alloc, &combined);
    if (req.filter_query) |*existing| json_helpers.deinitJsonValue(alloc, existing);
    req.filter_query = combined;
}

pub fn scanLineKey(alloc: std.mem.Allocator, line: []const u8) ![]u8 {
    const ScanLineKey = struct {
        key: []const u8,
    };
    var parsed = try std.json.parseFromSlice(ScanLineKey, alloc, line, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    return try alloc.dupe(u8, parsed.value.key);
}

const TestQueryHitInput = struct {
    _id: []const u8,
    _score: f32 = 0,
    _source: ?std.json.Value = null,
};

const OwnedJsonValueSlice = struct {
    values: []std.json.Value,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.values) |*value| ApiHttpServer.deinitJsonValue(alloc, value);
        if (self.values.len > 0) alloc.free(self.values);
        self.* = undefined;
    }
};

fn parseTestQueryHitsAlloc(alloc: std.mem.Allocator, json: []const u8) !OwnedJsonValueSlice {
    var parsed = try std.json.parseFromSlice([]const TestQueryHitInput, alloc, json, .{});
    defer parsed.deinit();

    const values = try alloc.alloc(std.json.Value, parsed.value.len);
    var initialized: usize = 0;
    errdefer {
        for (values[0..initialized]) |*value| ApiHttpServer.deinitJsonValue(alloc, value);
        if (values.len > 0) alloc.free(values);
    }

    for (parsed.value, 0..) |item, idx| {
        var obj = std.json.ObjectMap.empty;
        errdefer {
            var owned = std.json.Value{ .object = obj };
            ApiHttpServer.deinitJsonValue(alloc, &owned);
        }
        try obj.put(alloc, try alloc.dupe(u8, "_id"), .{ .string = try alloc.dupe(u8, item._id) });
        try obj.put(alloc, try alloc.dupe(u8, "_score"), .{ .float = item._score });
        if (item._source) |source| {
            try obj.put(alloc, try alloc.dupe(u8, "_source"), try ApiHttpServer.cloneJsonValue(alloc, source));
        }
        values[idx] = .{ .object = obj };
        initialized = idx + 1;
    }

    return .{ .values = values };
}

fn parseTestStringValuesAlloc(alloc: std.mem.Allocator, json: []const u8) !OwnedJsonValueSlice {
    var parsed = try std.json.parseFromSlice([]const []const u8, alloc, json, .{});
    defer parsed.deinit();

    const values = try alloc.alloc(std.json.Value, parsed.value.len);
    errdefer if (values.len > 0) alloc.free(values);
    for (parsed.value, 0..) |item, idx| {
        values[idx] = .{ .string = try alloc.dupe(u8, item) };
    }
    return .{ .values = values };
}

fn testQueryHitSourcePathValue(hit: anytype, path: []const u8) ?std.json.Value {
    const source = hit._source orelse return null;
    return json_helpers.extractJsonPathValue(source, path);
}

fn testOwnedHitSourcePathValue(hit: std.json.Value, path: []const u8) ?std.json.Value {
    const source = hit.object.get("_source") orelse return null;
    return json_helpers.extractJsonPathValue(source, path);
}

pub fn freeApiKeys(alloc: std.mem.Allocator, api_keys: []const usermgr.ApiKey) void {
    for (api_keys) |api_key| {
        var owned = api_key;
        owned.deinit(alloc);
    }
    alloc.free(@constCast(api_keys));
}

pub const ListedUserEntry = struct { username: []const u8 };

pub fn makeListedUsers(alloc: std.mem.Allocator, users: []const []const u8) ![]ListedUserEntry {
    const listed = try alloc.alloc(ListedUserEntry, users.len);
    for (users, 0..) |username, i| {
        listed[i] = .{ .username = username };
    }
    return listed;
}

pub fn userToOpenApi(alloc: std.mem.Allocator, user: usermgr.User) !usermgr_openapi.User {
    return .{
        .username = user.username,
        .password_hash = user.password_hash,
        .metadata = if (user.metadata_json.len > 0) try parseOwnedJsonObjectMapAlloc(alloc, user.metadata_json) else null,
    };
}

pub fn makeCurrentUserResponse(
    alloc: std.mem.Allocator,
    username: []const u8,
    permissions: []const usermgr.Permission,
    metadata_json: []const u8,
) !struct {
    username: []const u8,
    permissions: []const usermgr_openapi.Permission,
    metadata: ?std.json.ArrayHashMap(std.json.Value) = null,
} {
    const generated_permissions = try clonePermissionsToOpenApi(alloc, permissions);
    return .{
        .username = username,
        .permissions = generated_permissions,
        .metadata = if (metadata_json.len > 0) try parseOwnedJsonObjectMapAlloc(alloc, metadata_json) else null,
    };
}

fn eventStreamResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/event-stream"),
        .body = try alloc.dupe(u8, body),
    };
}

fn textResponse(alloc: std.mem.Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
}

pub fn buildLocalSchemaUpdateStatus(alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !struct {
    name: []const u8,
    schema: schema_openapi.TableSchema,
} {
    return .{
        .name = table_name,
        .schema = try std.json.parseFromSliceLeaky(schema_openapi.TableSchema, alloc, schema_json, .{
            .allocate = .alloc_always,
        }),
    };
}

pub fn stripApiPrefix(path: []const u8) []const u8 {
    const prefix = "/api/v1";
    if (std.mem.startsWith(u8, path, prefix)) {
        const rest = path[prefix.len..];
        // "/api/v1" alone or "/api/v1/" → "/"
        if (rest.len == 0) return "/";
        return rest;
    }
    return path;
}

const UriParts = struct { path: []const u8, query: []const u8 };

fn splitTarget(target: []const u8) UriParts {
    const query_index = std.mem.indexOfScalar(u8, target, '?') orelse return .{
        .path = stripApiPrefix(target),
        .query = "",
    };
    return .{
        .path = stripApiPrefix(target[0..query_index]),
        .query = target[query_index + 1 ..],
    };
}

fn rawPathOnly(target: []const u8) []const u8 {
    const query_index = std.mem.indexOfScalar(u8, target, '?') orelse return target;
    return target[0..query_index];
}

fn parseSimpleQueryParam(query: []const u8, key: []const u8) ?[]const u8 {
    if (query.len == 0) return null;
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |part| {
        if (!std.mem.startsWith(u8, part, key)) continue;
        if (part.len <= key.len or part[key.len] != '=') continue;
        return part[key.len + 1 ..];
    }
    return null;
}

pub fn runtimeSchemaDebugRequested(query: []const u8) bool {
    const value = parseSimpleQueryParam(query, "debug") orelse return false;
    return std.mem.eql(u8, value, "runtime_schema");
}

fn parseUnsignedQueryParam(query: []const u8, key: []const u8) !?u64 {
    const value = parseSimpleQueryParam(query, key) orelse return null;
    return try std.fmt.parseUnsigned(u64, value, 10);
}

test "api http server serves status" {
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 3,
                .rebalance_placement_groups = 1,
            };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);
    var resp = try server.handle(.{ .method = .GET, .uri = routes.Routes.status });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(cluster.ClusterStatus, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(cluster.ClusterHealth.healthy, parsed.value.health);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"data\"") == null);

    var cluster_resp = try server.handle(.{ .method = .GET, .uri = routes.Routes.cluster });
    defer cluster_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), cluster_resp.status);
    var parsed_cluster = try std.json.parseFromSlice(cluster.ClusterTopology, std.testing.allocator, cluster_resp.body, .{});
    defer parsed_cluster.deinit();
    try std.testing.expectEqual(cluster.ClusterHealth.healthy, parsed_cluster.value.health);
    try std.testing.expectEqual(@as(usize, 0), parsed_cluster.value.data.nodes.len);

    var healthz = try server.handle(.{ .method = .GET, .uri = routes.Routes.healthz });
    defer healthz.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), healthz.status);
    try std.testing.expectEqualStrings("application/json", healthz.content_type.?);
    try std.testing.expect(std.mem.indexOf(u8, healthz.body, "\"status\":\"ok\"") != null);

    var readyz = try server.handle(.{ .method = .GET, .uri = routes.Routes.readyz });
    defer readyz.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), readyz.status);
    try std.testing.expectEqualStrings("application/json", readyz.content_type.?);
    try std.testing.expect(std.mem.indexOf(u8, readyz.body, "\"status\":\"ready\"") != null);

    var prefixed_healthz = try server.handle(.{ .method = .GET, .uri = "/api/v1/healthz" });
    defer prefixed_healthz.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), prefixed_healthz.status);

    var prefixed_readyz = try server.handle(.{ .method = .GET, .uri = "/api/v1/readyz" });
    defer prefixed_readyz.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), prefixed_readyz.status);

    const request_stats = server.requestStats();
    try std.testing.expectEqual(@as(u64, 6), request_stats.request_count);
    try std.testing.expect(request_stats.first_request_started_at_ns >= server.created_at_ns);
}

test "api http server serves mcp and a2a protocol surfaces" {
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 77, .metrics = .{} };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);
    defer server.deinit();

    var mcp_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.mcp_v1,
        .content_type = "application/json",
        .body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{}}",
    });
    defer mcp_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), mcp_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, mcp_resp.body, "\"protocolVersion\":\"2025-06-18\"") != null);
    try std.testing.expectEqual(@as(usize, 2), mcp_resp.headers.len);
    try std.testing.expectEqualStrings("Mcp-Session-Id", mcp_resp.headers[0].name);
    try std.testing.expectEqualStrings("Mcp-Protocol-Version", mcp_resp.headers[1].name);
    try std.testing.expectEqualStrings("2025-06-18", mcp_resp.headers[1].value);
    const mcp_session_headers = [_]http_common.RequestHeader{
        .{ .name = mcp.session_id_header, .value = mcp_resp.headers[0].value },
    };

    var mcp_stream = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.mcp_v1,
        .headers = &mcp_session_headers,
    });
    defer mcp_stream.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), mcp_stream.status);
    try std.testing.expectEqualStrings("text/event-stream", mcp_stream.content_type.?);
    try std.testing.expect(std.mem.indexOf(u8, mcp_stream.body, "id: 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, mcp_stream.body, "event: endpoint") != null);

    const mcp_resume_headers = [_]http_common.RequestHeader{
        .{ .name = mcp.session_id_header, .value = mcp_resp.headers[0].value },
        .{ .name = mcp.last_event_id_header, .value = "9" },
    };
    var mcp_resumed_stream = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.mcp_v1,
        .headers = &mcp_resume_headers,
    });
    defer mcp_resumed_stream.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), mcp_resumed_stream.status);
    try std.testing.expect(std.mem.indexOf(u8, mcp_resumed_stream.body, "id: 10\n") != null);

    var mcp_delete = try server.handle(.{
        .method = .DELETE,
        .uri = routes.Routes.mcp_v1,
        .headers = &mcp_session_headers,
    });
    defer mcp_delete.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), mcp_delete.status);

    var mcp_missing_session = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.mcp_v1,
    });
    defer mcp_missing_session.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), mcp_missing_session.status);

    var card_resp = try server.handle(.{ .method = .GET, .uri = routes.Routes.agent_card });
    defer card_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), card_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, card_resp.body, "\"id\":\"query-builder\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, card_resp.body, "\"id\":\"retrieval\"") != null);

    var a2a_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.a2a,
        .content_type = "application/json",
        .body = "{\"jsonrpc\":\"2.0\",\"id\":\"card\",\"method\":\"agent/getAuthenticatedExtendedCard\",\"params\":{}}",
    });
    defer a2a_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), a2a_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, a2a_resp.body, "\"preferredTransport\":\"JSONRPC\"") != null);

    var stream_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.a2a,
        .content_type = "application/json",
        .body = "{\"jsonrpc\":\"2.0\",\"id\":\"stream\",\"method\":\"message/stream\",\"params\":{\"taskId\":\"t1\",\"contextId\":\"c1\",\"message\":{\"kind\":\"message\",\"role\":\"user\",\"metadata\":{\"skill\":\"query-builder\"},\"parts\":[{\"kind\":\"text\",\"text\":\"find docs\"}]}}}",
    });
    defer stream_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), stream_resp.status);
    try std.testing.expectEqualStrings("text/event-stream", stream_resp.content_type.?);
    try std.testing.expect(std.mem.indexOf(u8, stream_resp.body, "event: message") != null);
    try std.testing.expect(std.mem.indexOf(u8, stream_resp.body, "event: done") != null);

    var task_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.a2a,
        .content_type = "application/json",
        .body = "{\"jsonrpc\":\"2.0\",\"id\":\"get\",\"method\":\"tasks/get\",\"params\":{\"id\":\"t1\"}}",
    });
    defer task_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), task_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, task_resp.body, "\"id\":\"t1\"") != null);

    var cancel_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.a2a,
        .content_type = "application/json",
        .body = "{\"jsonrpc\":\"2.0\",\"id\":\"cancel\",\"method\":\"tasks/cancel\",\"params\":{\"id\":\"t1\"}}",
    });
    defer cancel_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), cancel_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, cancel_resp.body, "\"state\":\"canceled\"") != null);
}

fn encodeBasicAuthorization(alloc: std.mem.Allocator, username: []const u8, password: []const u8) ![]u8 {
    const raw = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ username, password });
    defer alloc.free(raw);
    const size = std.base64.standard.Encoder.calcSize(raw.len);
    const encoded = try alloc.alloc(u8, size);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, raw);
    return try std.fmt.allocPrint(alloc, "Basic {s}", .{encoded});
}

const TestAuthManager = struct {
    store: usermgr.MemoryStore,
    policy_store: casbin.MemoryAdapter,
    manager: usermgr.UserManager,
};

fn initTestAuthManager(alloc: std.mem.Allocator) !TestAuthManager {
    return .{
        .store = usermgr.MemoryStore.init(alloc),
        .policy_store = casbin.MemoryAdapter.init(alloc),
        .manager = undefined,
    };
}

fn bindTestAuthManager(alloc: std.mem.Allocator, auth: *TestAuthManager) !void {
    auth.manager = try usermgr.UserManager.init(
        alloc,
        auth.store.iface(),
        try usermgr.initDefaultEnforcer(alloc, auth.policy_store.iface()),
    );
}

test "api http server requires auth on public routes when enabled" {
    const ErrorResponse = struct {
        @"error": []const u8,
    };
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    var auth = try initTestAuthManager(std.testing.allocator);
    try bindTestAuthManager(std.testing.allocator, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var admin_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(std.testing.allocator, .@"*", "*", .admin),
    };
    defer admin_permission[0].deinit(std.testing.allocator);
    var admin_user = try auth.manager.createUser("admin", "admin", &admin_permission);
    defer admin_user.deinit(std.testing.allocator);

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{
        .auth_enabled = true,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);

    var unauthorized = try server.handle(.{ .method = .GET, .uri = routes.Routes.status });
    defer unauthorized.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 401), unauthorized.status);
    try std.testing.expectEqualStrings("application/json", unauthorized.content_type.?);
    try std.testing.expectEqual(@as(usize, 1), unauthorized.headers.len);
    try std.testing.expectEqualStrings("WWW-Authenticate", unauthorized.headers[0].name);
    try std.testing.expectEqualStrings("Basic realm=\"antfly\"", unauthorized.headers[0].value);
    var unauthorized_body = try std.json.parseFromSlice(ErrorResponse, std.testing.allocator, unauthorized.body, .{});
    defer unauthorized_body.deinit();
    try std.testing.expectEqualStrings("unauthorized", unauthorized_body.value.@"error");

    const admin_auth = try encodeBasicAuthorization(std.testing.allocator, "admin", "admin");
    defer std.testing.allocator.free(admin_auth);
    var authorized = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.status,
        .authorization = admin_auth,
    });
    defer authorized.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), authorized.status);

    var healthz = try server.handle(.{ .method = .GET, .uri = routes.Routes.healthz });
    defer healthz.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), healthz.status);

    var readyz = try server.handle(.{ .method = .GET, .uri = routes.Routes.readyz });
    defer readyz.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), readyz.status);
}

test "api http server auth fixture can be moved before binding" {
    const alloc = std.testing.allocator;

    var original = try initTestAuthManager(alloc);
    var moved = original;
    original = undefined;

    try bindTestAuthManager(alloc, &moved);
    defer moved.manager.deinit();
    defer moved.policy_store.deinit();
    defer moved.store.deinit();

    var created = try moved.manager.createUser("alice", "secret", &.{});
    defer created.deinit(alloc);
    try std.testing.expectEqualStrings("alice", created.username);

    var authed = try moved.manager.authenticateUser("alice", "secret");
    defer authed.deinit(alloc);
    try std.testing.expectEqualStrings("alice", authed.username);
}

test "api http server serves secrets crud when backed by a local store" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-http-{d}.json", .{platform_time.monotonicNs()});
    defer alloc.free(store_path);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    defer std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};

    var source = FakeSource{};
    var store = try common_secrets.FileStore.init(alloc, store_path);
    defer store.deinit();

    var server = ApiHttpServer.init(alloc, .{
        .swarm_mode = true,
        .secret_store = &store,
    }, source.iface(), null, null);

    var put_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/secrets/openai.api_key",
        .body = "{\"value\":\"sk-test\"}",
    });
    defer put_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), put_resp.status);
    try std.testing.expectEqualStrings("application/json", put_resp.content_type.?);
    var put_entry = try std.json.parseFromSlice(metadata_openapi.SecretEntry, alloc, put_resp.body, .{});
    defer put_entry.deinit();
    try std.testing.expectEqualStrings("openai.api_key", put_entry.value.key);
    try std.testing.expectEqual(metadata_openapi.SecretStatus.configured_keystore, put_entry.value.status);
    try std.testing.expectEqualStrings("OPENAI_API_KEY", put_entry.value.env_var.?);
    try std.testing.expect(put_entry.value.created_at != null);
    try std.testing.expect(put_entry.value.updated_at != null);

    var list_resp = try server.handle(.{
        .method = .GET,
        .uri = "/secrets",
    });
    defer list_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), list_resp.status);
    try std.testing.expectEqualStrings("application/json", list_resp.content_type.?);
    var list = try std.json.parseFromSlice(metadata_openapi.SecretList, alloc, list_resp.body, .{});
    defer list.deinit();
    var found_openai = false;
    for (list.value.secrets) |secret| {
        if (std.mem.eql(u8, secret.key, "openai.api_key")) {
            found_openai = true;
            break;
        }
    }
    try std.testing.expect(found_openai);

    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = store_path,
        .data = "{\"secrets\":[{\"key\":\"gemini.api_key\",\"value\":\"externally-managed\",\"created_at_ns\":1,\"updated_at_ns\":2}]}",
    });

    var external_list_resp = try server.handle(.{
        .method = .GET,
        .uri = "/secrets",
    });
    defer external_list_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), external_list_resp.status);
    var external_list = try std.json.parseFromSlice(metadata_openapi.SecretList, alloc, external_list_resp.body, .{});
    defer external_list.deinit();
    var found_external_gemini = false;
    var found_deleted_openai = false;
    for (external_list.value.secrets) |secret| {
        if (std.mem.eql(u8, secret.key, "gemini.api_key")) found_external_gemini = true;
        if (std.mem.eql(u8, secret.key, "openai.api_key")) found_deleted_openai = true;
    }
    try std.testing.expect(found_external_gemini);
    try std.testing.expect(!found_deleted_openai);

    var restored = try store.put(alloc, "openai.api_key", "sk-test");
    defer restored.deinit(alloc);

    var delete_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/secrets/openai.api_key",
    });
    defer delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), delete_resp.status);
}

test "api http server status includes secret store reload health" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-status-{d}.json", .{platform_time.monotonicNs()});
    defer alloc.free(store_path);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    defer std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};

    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = store_path,
        .data = "{\"secrets\":[{\"key\":\"openai.api_key\",\"value\":\"stable\",\"created_at_ns\":1,\"updated_at_ns\":1}]}",
    });

    var source = FakeSource{};
    var store = try common_secrets.FileStore.init(alloc, store_path);
    defer store.deinit();

    var server = ApiHttpServer.init(alloc, .{
        .swarm_mode = true,
        .secret_store = &store,
    }, source.iface(), null, null);

    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = store_path,
        .data = "{not-json",
    });

    var status_resp = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.status,
    });
    defer status_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), status_resp.status);
    var parsed = try std.json.parseFromSlice(cluster.ClusterStatus, alloc, status_resp.body, .{});
    defer parsed.deinit();
    const secret_store = parsed.value.secret_store orelse return error.TestUnexpectedResult;
    try std.testing.expect(secret_store.stale);
}

test "api http server lists secrets status without a local secret store" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    var list_resp = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.secrets,
    });
    defer list_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), list_resp.status);
    try std.testing.expectEqualStrings("application/json", list_resp.content_type.?);

    var list = try std.json.parseFromSlice(metadata_openapi.SecretList, alloc, list_resp.body, .{});
    defer list.deinit();
    const env_secrets = try common_secrets.listEnvironmentSecrets(alloc);
    defer common_secrets.freeListedSecrets(alloc, env_secrets);
    try std.testing.expectEqual(env_secrets.len, list.value.secrets.len);
    for (env_secrets, list.value.secrets) |expected, actual| {
        try std.testing.expectEqualStrings(expected.key, actual.key);
        try std.testing.expectEqual(metadata_openapi.SecretStatus.configured_env, actual.status);
        try std.testing.expectEqualStrings(expected.env_var.?, actual.env_var.?);
        try std.testing.expect(actual.created_at == null);
        try std.testing.expect(actual.updated_at == null);
    }
}

test "api http server forbids non-admin secret access when auth is enabled" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/test-secrets-auth-{d}.json", .{platform_time.monotonicNs()});
    defer alloc.free(store_path);
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    defer std.Io.Dir.cwd().deleteFile(io_impl.io(), store_path) catch {};

    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var read_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .table, "*", .read),
    };
    defer read_permission[0].deinit(alloc);
    var reader = try auth.manager.createUser("reader", "reader", &read_permission);
    defer reader.deinit(alloc);

    var source = FakeSource{};
    var store = try common_secrets.FileStore.init(alloc, store_path);
    defer store.deinit();

    var server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .swarm_mode = true,
        .secret_store = &store,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);

    const reader_auth = try encodeBasicAuthorization(alloc, "reader", "reader");
    defer alloc.free(reader_auth);
    var resp = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.secrets,
        .authorization = reader_auth,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 403), resp.status);
    try std.testing.expectEqualStrings("text/plain", resp.content_type.?);
    try std.testing.expectEqualStrings("forbidden", resp.body);
}

test "api http server query builder requires table read permission when auth is enabled" {
    const alloc = std.testing.allocator;
    const ErrorResponse = struct {
        @"error": []const u8,
    };
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 77, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var other_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .table, "other", .read),
    };
    defer other_permission[0].deinit(alloc);
    var reader = try auth.manager.createUser("reader", "reader", &other_permission);
    defer reader.deinit(alloc);

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);

    const reader_auth = try encodeBasicAuthorization(alloc, "reader", "reader");
    defer alloc.free(reader_auth);
    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .authorization = reader_auth,
        .body = "{\"table\":\"docs\",\"intent\":\"find raft docs\"}",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 403), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(ErrorResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("forbidden", parsed.value.@"error");
}

test "api http server restricts runtime schema debug to admins when auth is enabled" {
    const alloc = std.testing.allocator;
    const RuntimeSchemaDebugResponse = struct {
        debug: struct {
            runtime_schemas: []const struct {
                slot: []const u8,
                status: []const u8,
            },
        },
    };
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_tables = 1,
                .projected_ranges = 1,
                .projected_stores = 1,
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 77, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"version\":1,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\",\"x-antfly-types\":[\"text\"]}}}}}}",
                    .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var read_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .table, "docs", .read),
    };
    defer read_permission[0].deinit(alloc);
    var reader = try auth.manager.createUser("reader", "reader", &read_permission);
    defer reader.deinit(alloc);

    var admin_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .@"*", "*", .admin),
    };
    defer admin_permission[0].deinit(alloc);
    var admin = try auth.manager.createUser("admin", "admin", &admin_permission);
    defer admin.deinit(alloc);

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);

    const reader_auth = try encodeBasicAuthorization(alloc, "reader", "reader");
    defer alloc.free(reader_auth);
    var reader_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs?debug=runtime_schema",
        .authorization = reader_auth,
    });
    defer reader_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 403), reader_resp.status);
    try std.testing.expectEqualStrings("text/plain", reader_resp.content_type.?);
    try std.testing.expectEqualStrings("forbidden", reader_resp.body);

    const admin_auth = try encodeBasicAuthorization(alloc, "admin", "admin");
    defer alloc.free(admin_auth);
    var admin_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs?debug=runtime_schema",
        .authorization = admin_auth,
    });
    defer admin_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), admin_resp.status);
    try std.testing.expectEqualStrings("application/json", admin_resp.content_type.?);
    var parsed_admin_contract = try std.json.parseFromSlice(metadata_openapi.TableStatus, alloc, admin_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_admin_contract.deinit();
    try std.testing.expectEqualStrings("docs", parsed_admin_contract.value.name);
    var parsed_admin = try std.json.parseFromSlice(RuntimeSchemaDebugResponse, alloc, admin_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_admin.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_admin.value.debug.runtime_schemas.len);
    try std.testing.expectEqualStrings("active", parsed_admin.value.debug.runtime_schemas[0].slot);
    try std.testing.expectEqualStrings("read", parsed_admin.value.debug.runtime_schemas[1].slot);
    try std.testing.expectEqualStrings("ok", parsed_admin.value.debug.runtime_schemas[0].status);
}

test "api http server serves user management routes when auth is enabled" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var admin_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .@"*", "*", .admin),
    };
    defer admin_permission[0].deinit(alloc);
    var admin = try auth.manager.createUser("admin", "admin", &admin_permission);
    defer admin.deinit(alloc);

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);

    const admin_auth = try encodeBasicAuthorization(alloc, "admin", "admin");
    defer alloc.free(admin_auth);

    const CurrentUserResponse = struct {
        username: []const u8,
        permissions: []const usermgr_openapi.Permission,
        metadata: ?std.json.ArrayHashMap(std.json.Value) = null,
    };
    const ListedUserResponse = struct {
        username: []const u8,
    };

    var create_resp = try server.handle(.{
        .method = .POST,
        .uri = "/auth/v1/users/alice",
        .authorization = admin_auth,
        .body = "{\"password\":\"secret\",\"initial_policies\":[{\"resource\":\"docs\",\"resource_type\":\"table\",\"type\":\"read\"}],\"metadata\":{\"tenant_id\":\"acme\"}}",
    });
    defer create_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create_resp.status);
    try std.testing.expectEqualStrings("application/json", create_resp.content_type.?);
    var created_user = try std.json.parseFromSlice(usermgr_openapi.User, alloc, create_resp.body, .{});
    defer created_user.deinit();
    try std.testing.expectEqualStrings("alice", created_user.value.username);
    try std.testing.expectEqualStrings("acme", created_user.value.metadata.?.map.get("tenant_id").?.string);

    var me_resp = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.users_me,
        .authorization = admin_auth,
    });
    defer me_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), me_resp.status);
    try std.testing.expectEqualStrings("application/json", me_resp.content_type.?);

    var me = try std.json.parseFromSlice(CurrentUserResponse, alloc, me_resp.body, .{});
    defer me.deinit();
    try std.testing.expectEqualStrings("admin", me.value.username);

    var users_resp = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.users,
        .authorization = admin_auth,
    });
    defer users_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), users_resp.status);
    try std.testing.expectEqualStrings("application/json", users_resp.content_type.?);

    var users = try std.json.parseFromSlice([]ListedUserResponse, alloc, users_resp.body, .{});
    defer users.deinit();
    try std.testing.expectEqual(@as(usize, 2), users.value.len);

    var permissions_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice/permissions",
        .authorization = admin_auth,
    });
    defer permissions_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), permissions_resp.status);
    try std.testing.expectEqualStrings("application/json", permissions_resp.content_type.?);

    var permissions = try std.json.parseFromSlice([]usermgr_openapi.Permission, alloc, permissions_resp.body, .{});
    defer permissions.deinit();
    try std.testing.expectEqual(@as(usize, 1), permissions.value.len);
    try std.testing.expectEqualStrings("docs", permissions.value[0].resource);

    var user_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice",
        .authorization = admin_auth,
    });
    defer user_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), user_resp.status);
    try std.testing.expectEqualStrings("application/json", user_resp.content_type.?);
    var user = try std.json.parseFromSlice(usermgr_openapi.User, alloc, user_resp.body, .{});
    defer user.deinit();
    try std.testing.expectEqualStrings("alice", user.value.username);

    var add_permission_resp = try server.handle(.{
        .method = .POST,
        .uri = "/auth/v1/users/alice/permissions",
        .authorization = admin_auth,
        .body = "{\"resource\":\"reports\",\"resource_type\":\"table\",\"type\":\"write\"}",
    });
    defer add_permission_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), add_permission_resp.status);
    try std.testing.expectEqualStrings("application/json", add_permission_resp.content_type.?);
    var add_permission = try std.json.parseFromSlice(usermgr_openapi.SuccessMessage, alloc, add_permission_resp.body, .{});
    defer add_permission.deinit();
    try std.testing.expect(add_permission.value.message != null);
    try std.testing.expect(add_permission.value.message.?.len > 0);

    var add_role_resp = try server.handle(.{
        .method = .POST,
        .uri = "/auth/v1/users/alice/roles",
        .authorization = admin_auth,
        .body = "{\"role\":\"role:tenant_reader\"}",
    });
    defer add_role_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), add_role_resp.status);
    try std.testing.expectEqualStrings("application/json", add_role_resp.content_type.?);

    var roles_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice/roles",
        .authorization = admin_auth,
    });
    defer roles_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), roles_resp.status);
    try std.testing.expectEqualStrings("application/json", roles_resp.content_type.?);
    var roles = try std.json.parseFromSlice([]const []const u8, alloc, roles_resp.body, .{});
    defer roles.deinit();
    try std.testing.expectEqual(@as(usize, 1), roles.value.len);
    try std.testing.expectEqualStrings("role:tenant_reader", roles.value[0]);

    var subjects_resp = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.auth_subjects,
        .authorization = admin_auth,
    });
    defer subjects_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), subjects_resp.status);
    try std.testing.expectEqualStrings("application/json", subjects_resp.content_type.?);
    var subjects = try std.json.parseFromSlice([]AuthSubjectResponse, alloc, subjects_resp.body, .{});
    defer subjects.deinit();
    try std.testing.expect(subjects.value.len >= 2);

    var delete_role_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/auth/v1/users/alice/roles?role=role:tenant_reader",
        .authorization = admin_auth,
    });
    defer delete_role_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), delete_role_resp.status);

    var password_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/auth/v1/users/alice/password",
        .authorization = admin_auth,
        .body = "{\"new_password\":\"new-secret\"}",
    });
    defer password_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), password_resp.status);
    try std.testing.expectEqualStrings("application/json", password_resp.content_type.?);
    var password_result = try std.json.parseFromSlice(usermgr_openapi.SuccessMessage, alloc, password_resp.body, .{});
    defer password_result.deinit();
    try std.testing.expect(password_result.value.message != null);
    try std.testing.expect(password_result.value.message.?.len > 0);

    var authed = try auth.manager.authenticateUser("alice", "new-secret");
    defer authed.deinit(alloc);
    try std.testing.expectEqualStrings("alice", authed.username);

    var delete_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/auth/v1/users/alice",
        .authorization = admin_auth,
    });
    defer delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), delete_resp.status);

    try std.testing.expectError(error.UserNotFound, auth.manager.getUser("alice"));
}

test "api http server serves api key and row filter routes" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var admin_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .@"*", "*", .admin),
    };
    defer admin_permission[0].deinit(alloc);
    var admin = try auth.manager.createUser("admin", "admin", &admin_permission);
    defer admin.deinit(alloc);

    var alice_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .table, "docs", .read),
        try usermgr.Permission.initOwned(alloc, .table, "reports", .read),
    };
    defer {
        for (&alice_permission) |*permission| permission.deinit(alloc);
    }
    var alice = try auth.manager.createUser("alice", "secret", &alice_permission);
    defer alice.deinit(alloc);

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);

    const admin_auth = try encodeBasicAuthorization(alloc, "admin", "admin");
    defer alloc.free(admin_auth);

    var api_key_resp = try server.handle(.{
        .method = .POST,
        .uri = "/auth/v1/users/alice/api-keys",
        .authorization = admin_auth,
        .body = "{\"name\":\"ci\",\"permissions\":[{\"resource\":\"docs\",\"resource_type\":\"table\",\"type\":\"read\"}],\"row_filter\":{\"docs\":{\"term\":{\"tier\":\"gold\"}}},\"expires_in\":\"1h\"}",
    });
    defer api_key_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), api_key_resp.status);
    try std.testing.expectEqualStrings("application/json", api_key_resp.content_type.?);

    var created = try std.json.parseFromSlice(usermgr_openapi.ApiKeyWithSecret, alloc, api_key_resp.body, .{});
    defer created.deinit();
    try std.testing.expect(created.value.key_secret.len > 0);
    try std.testing.expect(created.value.encoded.len > 0);

    var list_api_keys_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice/api-keys",
        .authorization = admin_auth,
    });
    defer list_api_keys_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), list_api_keys_resp.status);
    try std.testing.expectEqualStrings("application/json", list_api_keys_resp.content_type.?);

    var api_keys = try std.json.parseFromSlice([]usermgr_openapi.ApiKey, alloc, list_api_keys_resp.body, .{});
    defer api_keys.deinit();
    try std.testing.expectEqual(@as(usize, 1), api_keys.value.len);
    const created_key_id = created.value.key_id;
    const created_encoded = created.value.encoded;
    try std.testing.expectEqualStrings(created_key_id, api_keys.value[0].key_id);

    const bearer_auth = try std.fmt.allocPrint(alloc, "Bearer {s}", .{created_encoded});
    defer alloc.free(bearer_auth);
    var bearer_status = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.status,
        .authorization = bearer_auth,
    });
    defer bearer_status.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), bearer_status.status);

    var put_subject_row_filter_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/auth/v1/subjects/role:tenant_reader/row-filters/docs",
        .authorization = admin_auth,
        .body = "{\"term\":{\"tenant_id\":{\"$auth\":\"metadata.tenant_id\"}}}",
    });
    defer put_subject_row_filter_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), put_subject_row_filter_resp.status);
    try std.testing.expectEqualStrings("application/json", put_subject_row_filter_resp.content_type.?);

    var subject_row_filters_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/subjects/role:tenant_reader/row-filters",
        .authorization = admin_auth,
    });
    defer subject_row_filters_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), subject_row_filters_resp.status);
    try std.testing.expectEqualStrings("application/json", subject_row_filters_resp.content_type.?);
    var subject_row_filters = try std.json.parseFromSlice([]usermgr_openapi.RowFilterEntry, alloc, subject_row_filters_resp.body, .{});
    defer subject_row_filters.deinit();
    try std.testing.expectEqual(@as(usize, 1), subject_row_filters.value.len);
    try std.testing.expectEqualStrings("docs", subject_row_filters.value[0].table);

    var put_row_filter_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/auth/v1/users/alice/row-filters/docs",
        .authorization = admin_auth,
        .body = "{\"term\":{\"department\":\"eng\"}}",
    });
    defer put_row_filter_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), put_row_filter_resp.status);
    try std.testing.expectEqualStrings("application/json", put_row_filter_resp.content_type.?);
    var created_row_filter = try std.json.parseFromSlice(usermgr_openapi.RowFilterEntry, alloc, put_row_filter_resp.body, .{});
    defer created_row_filter.deinit();
    try std.testing.expectEqualStrings("docs", created_row_filter.value.table);

    var row_filters_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice/row-filters",
        .authorization = admin_auth,
    });
    defer row_filters_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), row_filters_resp.status);
    try std.testing.expectEqualStrings("application/json", row_filters_resp.content_type.?);

    var row_filters = try std.json.parseFromSlice([]usermgr_openapi.RowFilterEntry, alloc, row_filters_resp.body, .{});
    defer row_filters.deinit();
    try std.testing.expectEqual(@as(usize, 1), row_filters.value.len);
    try std.testing.expectEqualStrings("docs", row_filters.value[0].table);

    var single_row_filter_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice/row-filters/docs",
        .authorization = admin_auth,
    });
    defer single_row_filter_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), single_row_filter_resp.status);
    try std.testing.expectEqualStrings("application/json", single_row_filter_resp.content_type.?);
    var single_row_filter = try std.json.parseFromSlice(usermgr_openapi.RowFilterEntry, alloc, single_row_filter_resp.body, .{});
    defer single_row_filter.deinit();
    try std.testing.expectEqualStrings("docs", single_row_filter.value.table);

    var delete_row_filter_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/auth/v1/users/alice/row-filters/docs",
        .authorization = admin_auth,
    });
    defer delete_row_filter_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), delete_row_filter_resp.status);

    var delete_subject_row_filter_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/auth/v1/subjects/role:tenant_reader/row-filters/docs",
        .authorization = admin_auth,
    });
    defer delete_subject_row_filter_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), delete_subject_row_filter_resp.status);

    var delete_permission_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/auth/v1/users/alice/permissions?resource=docs&resourceType=table",
        .authorization = admin_auth,
    });
    defer delete_permission_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), delete_permission_resp.status);

    var empty_permissions_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice/permissions",
        .authorization = admin_auth,
    });
    defer empty_permissions_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), empty_permissions_resp.status);
    try std.testing.expectEqualStrings("application/json", empty_permissions_resp.content_type.?);

    var empty_permissions = try std.json.parseFromSlice([]usermgr_openapi.Permission, alloc, empty_permissions_resp.body, .{});
    defer empty_permissions.deinit();
    try std.testing.expectEqual(@as(usize, 1), empty_permissions.value.len);
    try std.testing.expectEqualStrings("reports", empty_permissions.value[0].resource);

    const delete_api_key_uri = try std.fmt.allocPrint(alloc, "/auth/v1/users/alice/api-keys/{s}", .{created_key_id});
    defer alloc.free(delete_api_key_uri);
    var delete_api_key_resp = try server.handle(.{
        .method = .DELETE,
        .uri = delete_api_key_uri,
        .authorization = admin_auth,
    });
    defer delete_api_key_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), delete_api_key_resp.status);

    var empty_api_keys_resp = try server.handle(.{
        .method = .GET,
        .uri = "/auth/v1/users/alice/api-keys",
        .authorization = admin_auth,
    });
    defer empty_api_keys_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), empty_api_keys_resp.status);
    try std.testing.expectEqualStrings("application/json", empty_api_keys_resp.content_type.?);

    var empty_api_keys = try std.json.parseFromSlice([]usermgr_openapi.ApiKey, alloc, empty_api_keys_resp.body, .{});
    defer empty_api_keys.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty_api_keys.value.len);
}

test "api http server returns json user auth errors" {
    const alloc = std.testing.allocator;
    const ErrorResponse = struct {
        @"error": []const u8,
    };
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    var auth = try initTestAuthManager(alloc);
    try bindTestAuthManager(alloc, &auth);
    defer auth.manager.deinit();
    defer auth.policy_store.deinit();
    defer auth.store.deinit();

    var admin_permission = [_]usermgr.Permission{
        try usermgr.Permission.initOwned(alloc, .@"*", "*", .admin),
    };
    defer admin_permission[0].deinit(alloc);
    var admin = try auth.manager.createUser("admin", "admin", &admin_permission);
    defer admin.deinit(alloc);

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{
        .auth_enabled = true,
        .user_manager = &auth.manager,
    }, source.iface(), null, null);

    const admin_auth = try encodeBasicAuthorization(alloc, "admin", "admin");
    defer alloc.free(admin_auth);

    var bad_delete_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/auth/v1/users/alice/permissions?resource=docs&resource_type=table",
        .authorization = admin_auth,
    });
    defer bad_delete_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), bad_delete_resp.status);
    try std.testing.expectEqualStrings("application/json", bad_delete_resp.content_type.?);

    var bad_delete_body = try std.json.parseFromSlice(ErrorResponse, alloc, bad_delete_resp.body, .{});
    defer bad_delete_body.deinit();
    try std.testing.expectEqualStrings("missing resourceType", bad_delete_body.value.@"error");

    var bad_row_filter_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/auth/v1/users/alice/row-filters/docs",
        .authorization = admin_auth,
        .body = "[]",
    });
    defer bad_row_filter_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), bad_row_filter_resp.status);
    try std.testing.expectEqualStrings("application/json", bad_row_filter_resp.content_type.?);

    var bad_row_filter_body = try std.json.parseFromSlice(ErrorResponse, alloc, bad_row_filter_resp.body, .{});
    defer bad_row_filter_body.deinit();
    try std.testing.expectEqualStrings("invalid row filter", bad_row_filter_body.value.@"error");
}

test "api http server rejects secret writes without a local secret store" {
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_stores = 2,
            };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);
    var resp = try server.handle(.{
        .method = .PUT,
        .uri = "/secrets/openai.api_key",
        .body = "{\"value\":\"sk-test\"}",
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("text/plain", resp.content_type.?);
    try std.testing.expectEqualStrings("secret management not available in multi-node mode", resp.body);
}

test "api http server serves table lookup with version header" {
    const LookupResponse = struct {
        title: []const u8,
    };
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-lookup";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "doc:a",
                .value = "{\"title\":\"alpha\",\"body\":\"hello\"}",
            },
        },
        .timestamp_ns = 4321,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 1, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:z" }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), table_source.source(), null);
    var resp = try server.handle(.{ .method = .GET, .uri = "/tables/docs/lookup/doc:a?fields=title" });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(LookupResponse, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("alpha", parsed.value.title);
    try std.testing.expectEqual(@as(usize, 1), resp.headers.len);
    try std.testing.expectEqualStrings("X-Antfly-Version", resp.headers[0].name);
    try std.testing.expectEqualStrings("4321", resp.headers[0].value);
}

test "api http server decodes percent-encoded lookup keys" {
    const LookupResponse = struct {
        title: []const u8,
    };
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-lookup-encoded";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{
                .key = "docs/getting-started.md",
                .value = "{\"title\":\"alpha\",\"body\":\"hello\"}",
            },
        },
        .timestamp_ns = 4321,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 1, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = "" }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), table_source.source(), null);
    var resp = try server.handle(.{ .method = .GET, .uri = "/tables/docs/lookup/docs%2Fgetting-started.md?fields=title" });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(LookupResponse, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("alpha", parsed.value.title);
}

test "api http server serves table scan as ndjson" {
    const ScanRow = struct {
        key: []const u8,
        title: []const u8,
    };
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-scan";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), table_source.source(), null);
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/lookup",
        .content_type = "application/json",
        .body = "{\"from\":\"doc:a\",\"to\":\"doc:b\",\"inclusive_from\":true,\"fields\":[\"title\"]}",
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/x-ndjson", resp.content_type.?);
    const newline = std.mem.indexOfScalar(u8, resp.body, '\n') orelse resp.body.len;
    var parsed = try std.json.parseFromSlice(ScanRow, std.testing.allocator, resp.body[0..newline], .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("doc:a", parsed.value.key);
    try std.testing.expectEqualStrings("alpha", parsed.value.title);
}

test "api http server serves table query response envelope" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-query";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello\"}" }},
        .sync_level = .full_index,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), table_source.source(), null);
    const query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "hello", &.{}, 5);
    defer std.testing.allocator.free(query_body);
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/query",
        .content_type = "application/json",
        .body = query_body,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.responses.?.len);
    try std.testing.expectEqualStrings("doc:a", parsed.value.responses.?[0].hits.?.hits.?[0]._id);

    var internal_field_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/query",
        .content_type = "application/json",
        .body = "{\"query\":{\"match_all\":{}},\"_identity_read_generation\":1}",
    });
    defer internal_field_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), internal_field_resp.status);
    try std.testing.expectEqualStrings("invalid query request", internal_field_resp.body);
}

test "api http server serves table query with SearchAF-shaped terms aggregations" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-searchaf-aggregations";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"filename\":\"main.go\",\"content\":\"hello alpha\",\"extension\":\".go\",\"file_type\":\"source\"}" },
            .{ .key = "doc:b", .value = "{\"filename\":\"notes.txt\",\"content\":\"hello beta\",\"extension\":\".txt\",\"file_type\":\"text\"}" },
        },
        .sync_level = .full_index,
    });

    var table_source = table_reads.BoundTableReadSource.init("files", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), table_source.source(), null);
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/files/query",
        .content_type = "application/json",
        .body =
        \\{
        \\  "full_text_search": {
        \\    "disjuncts": [
        \\      {"match": "hello", "field": "content"},
        \\      {"match": "hello", "field": "filename", "boost": 3}
        \\    ],
        \\    "min": 1
        \\  },
        \\  "filter_query": {"term": ".go", "field": "extension"},
        \\  "fields": ["path", "directory", "extension", "file_type", "filename", "content"],
        \\  "limit": 5,
        \\  "aggregations": {
        \\    "file_types": {"type": "terms", "field": "file_type", "size": 10},
        \\    "extensions": {"type": "terms", "field": "extension", "size": 20}
        \\  }
        \\}
        ,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    const aggregations = parsed.value.responses.?[0].aggregations.?;
    try std.testing.expectEqual(@as(usize, 2), aggregations.map.count());
    try std.testing.expectEqualStrings("source", aggregations.map.get("file_types").?.buckets.?[0].key);
    try std.testing.expectEqual(@as(i64, 1), aggregations.map.get("file_types").?.buckets.?[0].doc_count);
    try std.testing.expectEqualStrings(".go", aggregations.map.get("extensions").?.buckets.?[0].key);
    try std.testing.expectEqual(@as(i64, 1), aggregations.map.get("extensions").?.buckets.?[0].doc_count);
}

test "api http server serves retrieval agent response envelope" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-retrieval";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello\"}" }},
        .sync_level = .full_index,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), table_source.source(), null);
    const retrieval_body =
        \\{"query":"find hello","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:hello"},"limit":5}]}
    ;
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/agents/retrieval",
        .content_type = "application/json",
        .body = retrieval_body,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed.value.status);
    try std.testing.expectEqualStrings("doc:a", parsed.value.hits[0]._id);
    try std.testing.expectEqual(metadata_openapi.RetrievalStrategy.bm25, parsed.value.strategy_used.?);

    const internal_query_body =
        \\{"query":"find hello","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:hello"},"native_doc_id_constraints":{"include_doc_ids":["doc:a"]},"limit":5}]}
    ;
    var internal_resp = try server.handle(.{
        .method = .POST,
        .uri = "/agents/retrieval",
        .content_type = "application/json",
        .body = internal_query_body,
    });
    defer internal_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), internal_resp.status);
    try std.testing.expectEqualStrings("invalid retrieval agent request", internal_resp.body);
}

test "api http server maps retrieval agent doc identity mismatch to unavailable" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeReads = struct {
        fn source(_: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) anyerror!?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) anyerror!?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) anyerror!?query_api.QueryResponse {
            return error.DocIdentityNamespaceMismatch;
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), null);
    const retrieval_body =
        \\{"query":"find hello","stream":false,"queries":[{"table":"docs","full_text_search":{"query":"body:hello"},"limit":5}]}
    ;
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/agents/retrieval",
        .content_type = "application/json",
        .body = retrieval_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("doc identity unavailable", resp.body);
}

test "api http server serves retrieval agent event stream" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-retrieval-stream";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"hello\"}" }},
        .sync_level = .full_index,
    });

    var table_source = table_reads.BoundTableReadSource.init("docs", 77, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), table_source.source(), null);
    const retrieval_body =
        \\{"query":"find hello","stream":true,"queries":[{"table":"docs","full_text_search":{"query":"body:hello"},"limit":5}]}
    ;
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/agents/retrieval",
        .content_type = "application/json",
        .body = retrieval_body,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("text/event-stream", resp.content_type.?);

    const events = try parseSseEventsAlloc(alloc, resp.body);
    defer alloc.free(events);
    try std.testing.expect(events.len >= 3);

    var saw_step_started = false;
    var saw_hit = false;
    var saw_done = false;
    for (events) |event| {
        if (std.mem.eql(u8, event.event, "step_started")) {
            saw_step_started = true;
        } else if (std.mem.eql(u8, event.event, "hit")) {
            saw_hit = true;
            var parsed_hit = try std.json.parseFromSlice(metadata_openapi.QueryHit, alloc, event.data, .{});
            defer parsed_hit.deinit();
            try std.testing.expectEqualStrings("doc:a", parsed_hit.value._id);
        } else if (std.mem.eql(u8, event.event, "done")) {
            saw_done = true;
            var parsed_done = try std.json.parseFromSlice(metadata_openapi.RetrievalAgentResult, alloc, event.data, .{});
            defer parsed_done.deinit();
            try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed_done.value.status);
            try std.testing.expectEqual(@as(usize, 1), parsed_done.value.hits.len);
            try std.testing.expectEqualStrings("doc:a", parsed_done.value.hits[0]._id);
        }
    }

    try std.testing.expect(saw_step_started);
    try std.testing.expect(saw_hit);
    try std.testing.expect(saw_done);

    var a2a_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.a2a,
        .content_type = "application/json",
        .body = "{\"jsonrpc\":\"2.0\",\"id\":\"retrieval-stream\",\"method\":\"message/stream\",\"params\":{\"taskId\":\"rt1\",\"contextId\":\"rc1\",\"message\":{\"kind\":\"message\",\"role\":\"user\",\"metadata\":{\"skill\":\"retrieval\"},\"parts\":[{\"kind\":\"text\",\"text\":\"find hello\"},{\"kind\":\"data\",\"data\":{\"table\":\"docs\",\"limit\":5}}]}}}",
    });
    defer a2a_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), a2a_resp.status);
    try std.testing.expectEqualStrings("text/event-stream", a2a_resp.content_type.?);
    try std.testing.expect(std.mem.indexOf(u8, a2a_resp.body, "\"event\":\"hit\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, a2a_resp.body, "\"name\":\"result\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, a2a_resp.body, "\"state\":\"completed\"") != null);
}

test "api http server serves eval response envelope" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    const eval_body =
        \\{"evaluators":["precision","recall","relevance","faithfulness"],"query":"How does raft consensus work?","output":"Raft uses leader election and replicated logs. [doc:1]","context":[{"title":"Raft","body":"Raft uses leader election and replicated logs."},{"title":"Other","body":"Unrelated content."}],"retrieved_ids":["doc:1","doc:2"],"ground_truth":{"relevant_ids":["doc:1"],"expectations":"leader election replicated logs"}}
    ;
    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.eval,
        .body = eval_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);

    var parsed = try std.json.parseFromSlice(eval_openapi.EvalResult, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.scores != null);
    try std.testing.expect(parsed.value.scores.?.retrieval != null);
    try std.testing.expect(parsed.value.scores.?.generation != null);
    try std.testing.expect(parsed.value.summary != null);
    try std.testing.expectEqual(@as(i64, 4), parsed.value.summary.?.total.?);
}

test "api http server serves query builder response envelope" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},\"status\":{\"type\":\"keyword\"}}}}}}",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    const query_builder_body =
        \\{"table":"docs","intent":"find published raft articles","mode":"auto","output":"query_request","constraints":{"limit":7},"max_internal_iterations":3,"max_user_clarifications":2}
    ;
    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = query_builder_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);

    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.session_id != null);
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, parsed.value.status.?);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.iteration.?);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.remaining_internal_iterations.?);
    try std.testing.expectEqual(@as(i64, 2), parsed.value.remaining_user_clarifications.?);
    try std.testing.expect(parsed.value.steps != null);
    try std.testing.expectEqualStrings("query_builder", parsed.value.steps.?[0].name);
    try std.testing.expect(parsed.value.query == .object);
    try std.testing.expect(parsed.value.query.object.get("conjuncts") != null);
    try std.testing.expect(parsed.value.query_request != null);
    try std.testing.expectEqualStrings("docs", parsed.value.query_request.?.table.?);
    try std.testing.expect(parsed.value.query_request.?.full_text_search != null);
    try std.testing.expect(parsed.value.query_request.?.filter_query != null);
    try std.testing.expectEqual(@as(i64, 7), parsed.value.query_request.?.limit.?);
    try std.testing.expectEqualStrings("full_text", parsed.value.specialist.?);
    try std.testing.expect(parsed.value.plan != null);
    try std.testing.expect(parsed.value.explanation != null);
}

test "api http server query builder infers semantic indexes from table metadata" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},\"status\":{\"type\":\"keyword\"}}}}}}",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":384},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs\",\"intent\":\"find raft architecture\",\"mode\":\"semantic\"}",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);

    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("semantic", parsed.value.specialist.?);
    try std.testing.expect(parsed.value.query_request != null);
    try std.testing.expectEqualStrings("find raft architecture", parsed.value.query_request.?.semantic_search.?);
    try std.testing.expectEqualStrings("semantic_idx", parsed.value.query_request.?.indexes.?[0]);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.query_request.?.indexes.?.len);
    try std.testing.expect(parsed.value.query_request.?.full_text_search == null);

    var invalid_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs\",\"intent\":\"find raft architecture\",\"mode\":\"semantic\",\"constraints\":{\"prefer_indexes\":[\"missing_idx\"],\"require_executable\":true}}",
    });
    defer invalid_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_resp.status);
}

test "api http server query builder maps doc identity mismatch to unavailable" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}}}}",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\",\"fields\":[\"title\",\"body\"]}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .preflight_query = preflightQuery,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) anyerror!?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) anyerror!?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn preflightQuery(
            _: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
            _: u32,
        ) anyerror!?db_mod.RuntimePreflightSummary {
            try std.testing.expectEqualStrings("docs", table_name);
            return error.DocIdentityNamespaceMismatch;
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), FakeReads.source(), null);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs\",\"intent\":\"find raft architecture\",\"mode\":\"full_text\",\"output\":\"query_request\"}",
    });
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    try std.testing.expectEqualStrings("{\"error\":\"doc identity unavailable\"}", resp.body);
}

test "api http server query builder loads structured table index metadata" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"}}}}}}",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\",\"fields\":[\"title\",\"body\"]},\"semantic_idx\":{\"type\":\"dense_vector\",\"dimension\":384,\"embedder\":{\"model\":\"e5-small\"}},\"sparse_idx\":{\"type\":\"sparse_vector\",\"model\":\"splade\"},\"doc_graph\":{\"type\":\"graph\",\"edge_types\":[{\"name\":\"references\",\"topology\":\"graph\"},{\"name\":\"parent\",\"topology\":\"tree\"}]}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);
    const context = try server.loadQueryBuilderTableContext("docs");
    defer freeQueryBuilderTableContext(alloc, context);

    try std.testing.expectEqualStrings("search_idx", context.full_text_index_metadata[0].name);
    try std.testing.expectEqualStrings("title", context.full_text_index_metadata[0].fields[0]);
    try std.testing.expectEqualStrings("body", context.full_text_index_metadata[0].fields[1]);

    try std.testing.expectEqual(@as(usize, 2), context.embedding_index_metadata.len);
    try std.testing.expectEqualStrings("semantic_idx", context.embedding_index_metadata[0].name);
    try std.testing.expect(!context.embedding_index_metadata[0].sparse);
    try std.testing.expectEqual(@as(?i64, 384), context.embedding_index_metadata[0].dimension);
    try std.testing.expectEqualStrings("e5-small", context.embedding_index_metadata[0].model.?);
    try std.testing.expectEqualStrings("sparse_idx", context.embedding_index_metadata[1].name);
    try std.testing.expect(context.embedding_index_metadata[1].sparse);

    try std.testing.expectEqualStrings("doc_graph", context.graph_index_metadata[0].name);
    try std.testing.expectEqual(@as(usize, 2), context.graph_index_metadata[0].edge_types.len);
    try std.testing.expectEqualStrings("references", context.graph_index_metadata[0].edge_types[0].name);
    try std.testing.expectEqualStrings("graph", context.graph_index_metadata[0].edge_types[0].topology.?);
    try std.testing.expectEqualStrings("parent", context.graph_index_metadata[0].edge_types[1].name);
    try std.testing.expectEqualStrings("tree", context.graph_index_metadata[0].edge_types[1].topology.?);
}

test "api http server query builder handles tree graph indexes" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 1,
                        .name = "docs_single",
                        .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"text\"}}}}}}",
                        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"doc_hierarchy\":{\"type\":\"graph\"}}",
                        .placement_role = "data",
                    },
                    .{
                        .table_id = 2,
                        .name = "docs_multi",
                        .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"body\":{\"type\":\"text\"}}}}}}",
                        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"doc_hierarchy\":{\"type\":\"graph\"},\"topic_graph\":{\"type\":\"graph\"}}",
                        .placement_role = "data",
                    },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null },
                    .{ .group_id = 20, .table_id = 2, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    var inferred_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs_single\",\"intent\":\"find raft architecture\",\"mode\":\"tree\"}",
    });
    defer inferred_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), inferred_resp.status);
    var inferred = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, inferred_resp.body, .{});
    defer inferred.deinit();
    try std.testing.expectEqualStrings("tree", inferred.value.specialist.?);
    try std.testing.expect(inferred.value.retrieval_query_request != null);
    try std.testing.expectEqualStrings("doc_hierarchy", inferred.value.retrieval_query_request.?.tree_search.?.index);

    var graph_inferred_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs_single\",\"intent\":\"find related raft architecture\",\"mode\":\"graph\"}",
    });
    defer graph_inferred_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), graph_inferred_resp.status);
    var graph_inferred = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, graph_inferred_resp.body, .{});
    defer graph_inferred.deinit();
    try std.testing.expectEqualStrings("graph", graph_inferred.value.specialist.?);
    const graph_query = graph_inferred.value.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqualStrings("doc_hierarchy", graph_query.index_name);
    try std.testing.expectEqualStrings("$full_text_results", graph_query.start_nodes.?.result_ref.?);

    var question_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs_multi\",\"intent\":\"find raft architecture\",\"mode\":\"tree\",\"interactive\":true,\"max_user_clarifications\":1}",
    });
    defer question_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), question_resp.status);
    var question = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, question_resp.body, .{});
    defer question.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.clarification_required, question.value.status.?);
    try std.testing.expectEqualStrings("select_tree_index", question.value.questions.?[0].id);

    var answer_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs_multi\",\"intent\":\"find raft architecture\",\"mode\":\"tree\",\"interactive\":true,\"max_user_clarifications\":1,\"decisions\":[{\"question_id\":\"select_tree_index\",\"answer\":\"topic_graph\"}]}",
    });
    defer answer_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), answer_resp.status);
    var answer = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, answer_resp.body, .{});
    defer answer.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, answer.value.status.?);
    try std.testing.expectEqualStrings("topic_graph", answer.value.retrieval_query_request.?.tree_search.?.index);

    var graph_question_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs_multi\",\"intent\":\"find related raft architecture\",\"mode\":\"graph\",\"interactive\":true,\"max_user_clarifications\":1}",
    });
    defer graph_question_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), graph_question_resp.status);
    var graph_question = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, graph_question_resp.body, .{});
    defer graph_question.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.clarification_required, graph_question.value.status.?);
    try std.testing.expectEqualStrings("select_graph_index", graph_question.value.questions.?[0].id);

    var graph_answer_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs_multi\",\"intent\":\"find related raft architecture\",\"mode\":\"graph\",\"interactive\":true,\"max_user_clarifications\":1,\"decisions\":[{\"question_id\":\"select_graph_index\",\"answer\":\"topic_graph\"}]}",
    });
    defer graph_answer_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), graph_answer_resp.status);
    var graph_answer = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, graph_answer_resp.body, .{});
    defer graph_answer.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, graph_answer.value.status.?);
    const graph_answer_query = graph_answer.value.query_request.?.graph_searches.?.map.get("graph_search").?;
    try std.testing.expectEqualStrings("topic_graph", graph_answer_query.index_name);
}

test "api http server query builder replays clarification decisions" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"body\":{\"type\":\"text\"},\"status\":{\"type\":\"keyword\"}}}}}}",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":384}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    var table_question_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"intent\":\"find raft architecture\",\"schema_fields\":[\"body\",\"status\"],\"interactive\":true,\"max_user_clarifications\":1,\"constraints\":{\"require_executable\":true}}",
    });
    defer table_question_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), table_question_resp.status);
    var table_question = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, table_question_resp.body, .{});
    defer table_question.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.clarification_required, table_question.value.status.?);
    try std.testing.expectEqualStrings("select_query_table", table_question.value.questions.?[0].id);

    var table_answer_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"intent\":\"find raft architecture\",\"schema_fields\":[\"body\",\"status\"],\"interactive\":true,\"max_user_clarifications\":1,\"constraints\":{\"require_executable\":true},\"decisions\":[{\"question_id\":\"select_query_table\",\"answer\":\"docs\"}]}",
    });
    defer table_answer_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), table_answer_resp.status);
    var table_answer = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, table_answer_resp.body, .{});
    defer table_answer.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, table_answer.value.status.?);
    try std.testing.expect(table_answer.value.questions == null);
    try std.testing.expectEqualStrings("docs", table_answer.value.query_request.?.table.?);

    var field_question_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"intent\":\"find raft architecture\",\"schema_fields\":[\"title\",\"body\",\"status\"],\"mode\":\"full_text\",\"interactive\":true,\"max_user_clarifications\":1}",
    });
    defer field_question_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), field_question_resp.status);
    var field_question = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, field_question_resp.body, .{});
    defer field_question.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.clarification_required, field_question.value.status.?);
    try std.testing.expectEqualStrings("select_text_field", field_question.value.questions.?[0].id);

    var field_answer_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"intent\":\"find raft architecture\",\"schema_fields\":[\"title\",\"body\",\"status\"],\"mode\":\"full_text\",\"interactive\":true,\"max_user_clarifications\":1,\"decisions\":[{\"question_id\":\"select_text_field\",\"answer\":\"title\"}]}",
    });
    defer field_answer_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), field_answer_resp.status);
    var field_answer = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, field_answer_resp.body, .{});
    defer field_answer.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, field_answer.value.status.?);
    try std.testing.expectEqualStrings("title", field_answer.value.query_request.?.full_text_search.?.object.get("field").?.string);

    var semantic_question_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"intent\":\"find raft architecture\",\"schema_fields\":[\"body\",\"status\"],\"mode\":\"semantic\",\"interactive\":true,\"max_user_clarifications\":1}",
    });
    defer semantic_question_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), semantic_question_resp.status);
    var semantic_question = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, semantic_question_resp.body, .{});
    defer semantic_question.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.clarification_required, semantic_question.value.status.?);
    try std.testing.expectEqualStrings("select_semantic_index", semantic_question.value.questions.?[0].id);

    var semantic_answer_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"intent\":\"find raft architecture\",\"schema_fields\":[\"body\",\"status\"],\"mode\":\"semantic\",\"interactive\":true,\"max_user_clarifications\":1,\"decisions\":[{\"question_id\":\"select_semantic_index\",\"answer\":\"body_embedding\"}]}",
    });
    defer semantic_answer_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), semantic_answer_resp.status);
    var semantic_answer = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, semantic_answer_resp.body, .{});
    defer semantic_answer.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, semantic_answer.value.status.?);
    try std.testing.expectEqualStrings("semantic", semantic_answer.value.specialist.?);
    try std.testing.expectEqualStrings("body_embedding", semantic_answer.value.query_request.?.indexes.?[0]);

    var strategy_question_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs\",\"intent\":\"find raft architecture\",\"mode\":\"auto\",\"interactive\":true,\"max_user_clarifications\":1}",
    });
    defer strategy_question_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), strategy_question_resp.status);
    var strategy_question = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, strategy_question_resp.body, .{});
    defer strategy_question.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.clarification_required, strategy_question.value.status.?);
    try std.testing.expectEqualStrings("select_query_strategy", strategy_question.value.questions.?[0].id);

    var strategy_answer_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"docs\",\"intent\":\"find raft architecture\",\"mode\":\"auto\",\"interactive\":true,\"max_user_clarifications\":1,\"decisions\":[{\"question_id\":\"select_query_strategy\",\"answer\":\"full_text\"}]}",
    });
    defer strategy_answer_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), strategy_answer_resp.status);
    var strategy_answer = try std.json.parseFromSlice(metadata_openapi.QueryBuilderResult, alloc, strategy_answer_resp.body, .{});
    defer strategy_answer.deinit();
    try std.testing.expectEqual(metadata_openapi.AgentStatus.completed, strategy_answer.value.status.?);
    try std.testing.expectEqualStrings("full_text", strategy_answer.value.specialist.?);
    try std.testing.expect(strategy_answer.value.query_request.?.semantic_search == null);
    try std.testing.expect(strategy_answer.value.query_request.?.full_text_search != null);
}

test "api http server returns json eval and query builder validation errors" {
    const alloc = std.testing.allocator;
    const ErrorResponse = struct {
        @"error": []const u8,
    };
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    var eval_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.eval,
        .body = "{}",
    });
    defer eval_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), eval_resp.status);
    try std.testing.expectEqualStrings("application/json", eval_resp.content_type.?);

    var eval_body = try std.json.parseFromSlice(ErrorResponse, alloc, eval_resp.body, .{});
    defer eval_body.deinit();
    try std.testing.expectEqualStrings("invalid eval request", eval_body.value.@"error");

    var query_builder_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"intent\":\"\"}",
    });
    defer query_builder_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), query_builder_resp.status);
    try std.testing.expectEqualStrings("application/json", query_builder_resp.content_type.?);

    var query_builder_body = try std.json.parseFromSlice(ErrorResponse, alloc, query_builder_resp.body, .{});
    defer query_builder_body.deinit();
    try std.testing.expectEqualStrings("invalid query builder request", query_builder_body.value.@"error");
}

test "api http server returns json not found for missing query builder table" {
    const alloc = std.testing.allocator;
    const ErrorResponse = struct {
        @"error": []const u8,
    };
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.agents_query_builder,
        .body = "{\"table\":\"missing\",\"intent\":\"find docs\"}",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);

    var parsed = try std.json.parseFromSlice(ErrorResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("not found", parsed.value.@"error");
}

test "api http server routes table query through read schema full text index" {
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"version\":1}",
                    .read_schema_json = "{\"version\":0}",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:z" }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        fn source(_: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return null;
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8, req: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(req.index_name != null);
            try std.testing.expectEqualStrings("full_text_index_v0", req.index_name.?);
            return .{
                .json = try inner_alloc.dupe(u8, "{\"responses\":[]}"),
            };
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), reads.source(), null);
    const query_body = try test_contract_helpers.encodeMatchQueryRequest(std.testing.allocator, "body", "hello", &.{}, 5);
    defer std.testing.allocator.free(query_body);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/query",
        .content_type = "application/json",
        .body = query_body,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.responses.?.len);
}

test "api http server serves table batch writes" {
    const alloc = std.testing.allocator;
    const StoredTitle = struct {
        title: []const u8,
    };
    const path = "/tmp/antfly-api-http-batch";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, table_source.source());
    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\"}},\"deletes\":[\"doc:gone\"]}");
    defer std.testing.allocator.free(batch_body);
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/batch",
        .content_type = "application/json",
        .body = batch_body,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, resp.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_batch.value.inserted.?);
    try std.testing.expectEqual(@as(i64, 1), parsed_batch.value.deleted.?);

    var lookup = (try db.lookup(alloc, "doc:a", .{})).?;
    defer lookup.deinit(alloc);
    var parsed_stored = try std.json.parseFromSlice(StoredTitle, alloc, lookup.json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed_stored.deinit();
    const stored = parsed_stored.value;
    try std.testing.expectEqualStrings("alpha", stored.title);

    var compact_batch: std.Io.Writer.Allocating = .init(alloc);
    defer compact_batch.deinit();
    const compact_writer = &compact_batch.writer;
    try compact_writer.writeAll("{\"inserts\":{");
    for (0..500) |i| {
        if (i != 0) try compact_writer.writeByte(',');
        try compact_writer.print(
            "\"key:{d}\":{{\"id\":{d},\"metadata\":{{\"source\":\"vdbbench\",\"ordinal\":{d}}},\"vec_data\":[0.1,0.2,0.3],\"_embeddings\":{{\"vec\":[0.1,0.2,0.3]}}}}",
            .{ i, i, i },
        );
    }
    try compact_writer.writeAll("},\"sync_level\":\"write\"}");
    var compact_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/batch",
        .content_type = "application/json",
        .body = compact_batch.written(),
    });
    defer compact_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), compact_resp.status);
    var parsed_compact = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, compact_resp.body, .{});
    defer parsed_compact.deinit();
    try std.testing.expectEqual(@as(i64, 500), parsed_compact.value.inserted.?);

    var compact_lookup = (try db.lookup(alloc, "key:0", .{})).?;
    defer compact_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, compact_lookup.json, "\"vec_data\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, compact_lookup.json, "_embeddings") == null);
}

test "api http server serves table batch transforms" {
    const alloc = std.testing.allocator;
    const StoredTransform = struct {
        status: []const u8,
        version: i64,
    };
    const path = "/tmp/antfly-api-http-batch-transform";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, table_source.source());

    const insert_body = try test_contract_helpers.normalizeBatchRequest(
        std.testing.allocator,
        "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\",\"version\":1}}}",
    );
    defer std.testing.allocator.free(insert_body);
    var insert_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/batch",
        .content_type = "application/json",
        .body = insert_body,
    });
    defer insert_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), insert_resp.status);
    try std.testing.expectEqualStrings("application/json", insert_resp.content_type.?);
    var parsed_insert = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, insert_resp.body, .{});
    defer parsed_insert.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_insert.value.inserted.?);

    const transform_body = try test_contract_helpers.normalizeBatchRequest(
        std.testing.allocator,
        "{\"transforms\":[{\"key\":\"doc:a\",\"operations\":[{\"op\":\"$set\",\"path\":\"status\",\"value\":\"updated\"},{\"op\":\"$max\",\"path\":\"version\",\"value\":3}]}]}",
    );
    defer std.testing.allocator.free(transform_body);
    var transform_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/batch",
        .content_type = "application/json",
        .body = transform_body,
    });
    defer transform_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), transform_resp.status);
    try std.testing.expectEqualStrings("application/json", transform_resp.content_type.?);

    var parsed_batch = try std.json.parseFromSlice(metadata_openapi.BatchResponse, std.testing.allocator, transform_resp.body, .{});
    defer parsed_batch.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed_batch.value.transformed.?);

    var lookup = (try db.lookup(alloc, "doc:a", .{})).?;
    defer lookup.deinit(alloc);
    var parsed_stored = try std.json.parseFromSlice(StoredTransform, alloc, lookup.json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed_stored.deinit();
    const stored = parsed_stored.value;
    try std.testing.expectEqualStrings("updated", stored.status);
    try std.testing.expectEqual(@as(i64, 3), stored.version);
}

test "api http server updates local table schema through bound write source" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/local-schema", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, table_source.source());

    const schema_body = "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"status\":{\"type\":\"keyword\"}}}}}}";
    var update_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/tables/docs/schema",
        .content_type = "application/json",
        .body = schema_body,
    });
    defer update_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), update_resp.status);
    try std.testing.expectEqualStrings("application/json", update_resp.content_type.?);
    var parsed_update = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, update_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_update.deinit();
    try std.testing.expect(parsed_update.value == .object);
    try std.testing.expectEqualStrings("docs", parsed_update.value.object.get("name").?.string);
    try std.testing.expect(parsed_update.value.object.get("schema") != null);

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"alpha\",\"body\":\"unexpected\"}}}");
    defer std.testing.allocator.free(batch_body);
    var batch_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/batch",
        .content_type = "application/json",
        .body = batch_body,
    });
    defer batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), batch_resp.status);
}

test "api http server serves public transaction commit route" {
    const alloc = std.testing.allocator;
    const StoredTitle = struct {
        title: []const u8,
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/public-txn", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 7,
    });

    var read_source = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), read_source.source(), table_source.source());

    const commit_batch = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"beta\"}}}");
    defer std.testing.allocator.free(commit_batch);
    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{.{ .table_name = "docs", .key = "doc:a", .version = "7" }},
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        "write",
    );
    defer std.testing.allocator.free(commit_body);

    var commit_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_commit,
        .content_type = "application/json",
        .body = commit_body,
    });
    defer commit_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), commit_resp.status);
    try std.testing.expectEqualStrings("application/json", commit_resp.content_type.?);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.testing.allocator, commit_resp.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);
    try std.testing.expect(parsed_commit.value.tables != null);

    var updated = (try db.lookup(alloc, "doc:a", .{})).?;
    defer updated.deinit(alloc);
    var parsed_stored = try std.json.parseFromSlice(StoredTitle, alloc, updated.json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed_stored.deinit();
    const stored = parsed_stored.value;
    try std.testing.expectEqualStrings("beta", stored.title);

    const stale_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{.{ .table_name = "docs", .key = "doc:a", .version = "7" }},
        &.{.{ .table_name = "docs", .batch_json = commit_batch }},
        null,
    );
    defer std.testing.allocator.free(stale_body);

    var stale_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_commit,
        .content_type = "application/json",
        .body = stale_body,
    });
    defer stale_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), stale_resp.status);
    try std.testing.expectEqualStrings("application/json", stale_resp.content_type.?);
    var parsed_conflict = try std.json.parseFromSlice(transactions_api.CommitResponse, std.testing.allocator, stale_resp.body, .{});
    defer parsed_conflict.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_conflict.value.status);
    const conflict = parsed_conflict.value.conflict.?;
    try std.testing.expectEqualStrings("docs", conflict.table);
    try std.testing.expectEqualStrings("doc:a", conflict.key);
    try std.testing.expectEqual(@as(?u64, 7), conflict.expected_version);
    try std.testing.expect(conflict.current_version.? >= 8);
    try std.testing.expectEqualStrings("version_conflict", conflict.kind);
    try std.testing.expect(conflict.participant == null);
}

test "api http server surfaces structured participant diagnostics for unavailable transaction commits" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeWrites = struct {
        fn source(_: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = batch,
                    .commit_transaction = commitTransaction,
                    .commit_transaction_with_id = commitTransactionWithId,
                    .txn_begin_group_local = beginGroup,
                    .txn_prepare_group_local = prepareGroup,
                    .txn_resolve_group_local = resolveGroup,
                    .txn_status_group_local = statusGroup,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn commitTransaction(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return .{
                .conflict = .{
                    .table_name = "docs",
                    .key = "",
                    .message = "participant unavailable",
                    .group_id = 7001,
                    .phase = .prepare,
                },
            };
        }

        fn commitTransactionWithId(
            ptr: *anyopaque,
            txn_alloc: std.mem.Allocator,
            _: db_mod.types.TxnId,
            _: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return commitTransaction(ptr, txn_alloc, tables, sync_level);
        }

        fn beginGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn prepareGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn resolveGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn statusGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) anyerror!?db_mod.types.TxnStatus {
            return error.UnsupportedOperation;
        }
    };

    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, writes.source());

    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        alloc,
        &.{},
        &.{.{ .table_name = "docs", .batch_json = "{\"inserts\":{}}" }},
        null,
    );
    defer alloc.free(commit_body);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_commit,
        .content_type = "application/json",
        .body = commit_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(transactions_api.CommitResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("participant_unavailable", conflict.kind);
    try std.testing.expectEqual(true, conflict.retryable);
    try std.testing.expectEqual(@as(?u32, 50), conflict.retry_after_ms);
    try std.testing.expectEqualStrings("participant", conflict.retry_scope.?);
    try std.testing.expect(conflict.participant != null);
    try std.testing.expectEqual(@as(?u64, 7001), conflict.participant.?.group_id);
    try std.testing.expectEqualStrings("prepare", conflict.participant.?.phase.?);
}

test "api http server surfaces structured decision conflicts for transaction commits" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeWrites = struct {
        fn source(_: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = batch,
                    .commit_transaction = commitTransaction,
                    .commit_transaction_with_id = commitTransactionWithId,
                    .txn_begin_group_local = beginGroup,
                    .txn_prepare_group_local = prepareGroup,
                    .txn_resolve_group_local = resolveGroup,
                    .txn_status_group_local = statusGroup,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn commitTransaction(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return error.DecisionConflict;
        }

        fn commitTransactionWithId(
            ptr: *anyopaque,
            txn_alloc: std.mem.Allocator,
            _: db_mod.types.TxnId,
            _: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return commitTransaction(ptr, txn_alloc, tables, sync_level);
        }

        fn beginGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn prepareGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn resolveGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn statusGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) anyerror!?db_mod.types.TxnStatus {
            return error.UnsupportedOperation;
        }
    };

    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.source());

    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        alloc,
        &.{},
        &.{.{ .table_name = "docs", .batch_json = "{\"inserts\":{}}" }},
        null,
    );
    defer alloc.free(commit_body);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_commit,
        .content_type = "application/json",
        .body = commit_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(transactions_api.CommitResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("aborted", parsed.value.status);
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("decision conflict", conflict.message);
    try std.testing.expectEqualStrings("transaction_conflict", conflict.kind);
    try std.testing.expectEqual(false, conflict.retryable);
    try std.testing.expectEqualStrings("docs", conflict.table);
}

test "api http server surfaces structured doc identity conflicts for transaction commits" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeWrites = struct {
        fn source(_: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = batch,
                    .commit_transaction = commitTransaction,
                    .commit_transaction_with_id = commitTransactionWithId,
                    .txn_begin_group_local = beginGroup,
                    .txn_prepare_group_local = prepareGroup,
                    .txn_resolve_group_local = resolveGroup,
                    .txn_status_group_local = statusGroup,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn commitTransaction(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return error.DocIdentityNamespaceMismatch;
        }

        fn commitTransactionWithId(
            ptr: *anyopaque,
            txn_alloc: std.mem.Allocator,
            _: db_mod.types.TxnId,
            _: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return commitTransaction(ptr, txn_alloc, tables, sync_level);
        }

        fn beginGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn prepareGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn resolveGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn statusGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) anyerror!?db_mod.types.TxnStatus {
            return error.UnsupportedOperation;
        }
    };

    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.source());

    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        alloc,
        &.{},
        &.{.{ .table_name = "docs", .batch_json = "{\"inserts\":{}}" }},
        null,
    );
    defer alloc.free(commit_body);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_commit,
        .content_type = "application/json",
        .body = commit_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(transactions_api.CommitResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("aborted", parsed.value.status);
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("doc identity unavailable", conflict.message);
    try std.testing.expectEqualStrings("doc_identity_unavailable", conflict.kind);
    try std.testing.expectEqual(true, conflict.retryable);
    try std.testing.expectEqual(@as(?u32, 100), conflict.retry_after_ms);
    try std.testing.expectEqualStrings("doc_identity", conflict.retry_scope.?);
    try std.testing.expectEqualStrings("docs", conflict.table);
}

test "api http server surfaces structured torn-state conflicts when txn record is missing" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeWrites = struct {
        fn source(_: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = batch,
                    .commit_transaction = commitTransaction,
                    .commit_transaction_with_id = commitTransactionWithId,
                    .txn_begin_group_local = beginGroup,
                    .txn_prepare_group_local = prepareGroup,
                    .txn_resolve_group_local = resolveGroup,
                    .txn_status_group_local = statusGroup,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn commitTransaction(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return error.TxnNotFound;
        }

        fn commitTransactionWithId(
            ptr: *anyopaque,
            txn_alloc: std.mem.Allocator,
            _: db_mod.types.TxnId,
            _: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return commitTransaction(ptr, txn_alloc, tables, sync_level);
        }

        fn beginGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn prepareGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn resolveGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn statusGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) anyerror!?db_mod.types.TxnStatus {
            return error.UnsupportedOperation;
        }
    };

    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.source());

    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        alloc,
        &.{},
        &.{.{ .table_name = "docs", .batch_json = "{\"inserts\":{}}" }},
        null,
    );
    defer alloc.free(commit_body);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_commit,
        .content_type = "application/json",
        .body = commit_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(transactions_api.CommitResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("aborted", parsed.value.status);
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("torn_state", conflict.kind);
    try std.testing.expectEqual(false, conflict.retryable);
    try std.testing.expectEqualStrings("docs", conflict.table);
}

test "api http server surfaces structured torn-state conflicts when txn record is corrupted" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeWrites = struct {
        fn source(_: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = batch,
                    .commit_transaction = commitTransaction,
                    .commit_transaction_with_id = commitTransactionWithId,
                    .txn_begin_group_local = beginGroup,
                    .txn_prepare_group_local = prepareGroup,
                    .txn_resolve_group_local = resolveGroup,
                    .txn_status_group_local = statusGroup,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn commitTransaction(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const distributed_txn.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return error.InvalidTxnRecord;
        }

        fn commitTransactionWithId(
            ptr: *anyopaque,
            txn_alloc: std.mem.Allocator,
            _: db_mod.types.TxnId,
            _: u64,
            tables: []const distributed_txn.TableCommitRequest,
            sync_level: db_mod.types.SyncLevel,
        ) anyerror!?distributed_txn.CommitOutcome {
            return commitTransaction(ptr, txn_alloc, tables, sync_level);
        }

        fn beginGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn prepareGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn resolveGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn statusGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) anyerror!?db_mod.types.TxnStatus {
            return error.UnsupportedOperation;
        }
    };

    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.source());

    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        alloc,
        &.{},
        &.{.{ .table_name = "docs", .batch_json = "{\"inserts\":{}}" }},
        null,
    );
    defer alloc.free(commit_body);

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_commit,
        .content_type = "application/json",
        .body = commit_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);
    var parsed = try std.json.parseFromSlice(transactions_api.CommitResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("aborted", parsed.value.status);
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("torn_state", conflict.kind);
    try std.testing.expectEqual(false, conflict.retryable);
    try std.testing.expectEqualStrings("docs", conflict.table);
}

test "api http server serves long-lived public transaction session routes" {
    const SessionCommitResponse = transactions_api.SessionCommitResponse;
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-session-txn";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 7,
    });

    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var read_source = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), read_source.source(), table_source.source());
    defer server.deinit();

    var begin_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{\"sync_level\":\"write\"}",
    });
    defer begin_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), begin_resp.status);
    try std.testing.expectEqualStrings("application/json", begin_resp.content_type.?);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, std.testing.allocator, begin_resp.body, .{});
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_stage_body = try test_contract_helpers.encodeTransactionStageReadRequest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "7",
    );
    defer std.testing.allocator.free(read_stage_body);
    const write_stage_body = try test_contract_helpers.encodeTransactionStageWriteRequest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "{\"title\":\"gamma\"}",
    );
    defer std.testing.allocator.free(write_stage_body);

    const read_stage_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_read_suffix,
    });
    defer std.testing.allocator.free(read_stage_uri);
    const write_stage_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_write_suffix,
    });
    defer std.testing.allocator.free(write_stage_uri);
    const commit_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_commit_suffix,
    });
    defer std.testing.allocator.free(commit_uri);
    const savepoint_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_savepoints_suffix,
    });
    defer std.testing.allocator.free(savepoint_uri);
    const delete_stage_uri_committed = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_delete_suffix,
    });
    defer std.testing.allocator.free(delete_stage_uri_committed);

    var read_stage_resp = try server.handle(.{
        .method = .POST,
        .uri = read_stage_uri,
        .content_type = "application/json",
        .body = read_stage_body,
    });
    defer read_stage_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), read_stage_resp.status);
    try std.testing.expectEqualStrings("application/json", read_stage_resp.content_type.?);
    var parsed_read_stage = try std.json.parseFromSlice(transactions_api.StageReadResponse, std.testing.allocator, read_stage_resp.body, .{});
    defer parsed_read_stage.deinit();
    try std.testing.expectEqualStrings("staged", parsed_read_stage.value.status);
    try std.testing.expectEqualStrings("7", parsed_read_stage.value.snapshot.version);
    try std.testing.expectEqualStrings("alpha", parsed_read_stage.value.snapshot.document.object.get("title").?.string);

    var write_stage_resp = try server.handle(.{
        .method = .POST,
        .uri = write_stage_uri,
        .content_type = "application/json",
        .body = write_stage_body,
    });
    defer write_stage_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), write_stage_resp.status);
    try std.testing.expectEqualStrings("application/json", write_stage_resp.content_type.?);
    var parsed_write_stage = try std.json.parseFromSlice(transactions_api.TransactionStatusResponse, std.testing.allocator, write_stage_resp.body, .{});
    defer parsed_write_stage.deinit();
    try std.testing.expectEqualStrings("staged", parsed_write_stage.value.status);

    var savepoint_resp = try server.handle(.{
        .method = .POST,
        .uri = savepoint_uri,
        .body = "",
    });
    defer savepoint_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), savepoint_resp.status);
    try std.testing.expectEqualStrings("application/json", savepoint_resp.content_type.?);
    var parsed_savepoint = try std.json.parseFromSlice(transactions_api.SavepointStatusResponse, std.testing.allocator, savepoint_resp.body, .{});
    defer parsed_savepoint.deinit();
    const savepoint_id = parsed_savepoint.value.savepoint_id;

    var delete_after_savepoint_resp = try server.handle(.{
        .method = .POST,
        .uri = delete_stage_uri_committed,
        .content_type = "application/json",
        .body = "{\"table\":\"docs\",\"key\":\"doc:a\"}",
    });
    defer delete_after_savepoint_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), delete_after_savepoint_resp.status);
    try std.testing.expectEqualStrings("application/json", delete_after_savepoint_resp.content_type.?);
    var parsed_delete_after_savepoint = try std.json.parseFromSlice(transactions_api.TransactionStatusResponse, std.testing.allocator, delete_after_savepoint_resp.body, .{});
    defer parsed_delete_after_savepoint.deinit();
    try std.testing.expectEqualStrings("staged", parsed_delete_after_savepoint.value.status);

    const rollback_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}/{d}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_savepoints_suffix,
        savepoint_id,
        routes.Routes.transactions_rollback_suffix,
    });
    defer std.testing.allocator.free(rollback_uri);
    var rollback_resp = try server.handle(.{
        .method = .POST,
        .uri = rollback_uri,
        .body = "",
    });
    defer rollback_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), rollback_resp.status);
    try std.testing.expectEqualStrings("application/json", rollback_resp.content_type.?);
    var parsed_rollback = try std.json.parseFromSlice(transactions_api.SavepointStatusResponse, std.testing.allocator, rollback_resp.body, .{});
    defer parsed_rollback.deinit();
    try std.testing.expectEqualStrings("rolled_back", parsed_rollback.value.status);

    var commit_resp = try server.handle(.{
        .method = .POST,
        .uri = commit_uri,
        .content_type = "application/json",
        .body = "",
    });
    defer commit_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), commit_resp.status);
    try std.testing.expectEqualStrings("application/json", commit_resp.content_type.?);
    var parsed_commit = try std.json.parseFromSlice(SessionCommitResponse, std.testing.allocator, commit_resp.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);
    try std.testing.expect(parsed_commit.value.tables != null);

    var commit_again = try server.handle(.{
        .method = .POST,
        .uri = commit_uri,
        .content_type = "application/json",
        .body = "",
    });
    defer commit_again.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), commit_again.status);

    var abort_begin = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{}",
    });
    defer abort_begin.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("application/json", abort_begin.content_type.?);
    var parsed_abort_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, std.testing.allocator, abort_begin.body, .{});
    defer parsed_abort_begin.deinit();
    const abort_txn_id_hex = parsed_abort_begin.value.transaction_id;
    const delete_stage_body = try test_contract_helpers.encodeTransactionStageDeleteRequest(
        std.testing.allocator,
        "docs",
        "doc:a",
    );
    defer std.testing.allocator.free(delete_stage_body);
    const delete_stage_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        abort_txn_id_hex,
        routes.Routes.transactions_delete_suffix,
    });
    defer std.testing.allocator.free(delete_stage_uri);
    var delete_stage_resp = try server.handle(.{
        .method = .POST,
        .uri = delete_stage_uri,
        .content_type = "application/json",
        .body = delete_stage_body,
    });
    defer delete_stage_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), delete_stage_resp.status);
    try std.testing.expectEqualStrings("application/json", delete_stage_resp.content_type.?);
    var parsed_delete_stage = try std.json.parseFromSlice(transactions_api.TransactionStatusResponse, std.testing.allocator, delete_stage_resp.body, .{});
    defer parsed_delete_stage.deinit();
    try std.testing.expectEqualStrings("staged", parsed_delete_stage.value.status);
    const abort_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        abort_txn_id_hex,
        routes.Routes.transactions_abort_suffix,
    });
    defer std.testing.allocator.free(abort_uri);
    var abort_resp = try server.handle(.{
        .method = .POST,
        .uri = abort_uri,
        .body = "",
    });
    defer abort_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), abort_resp.status);
    try std.testing.expectEqualStrings("application/json", abort_resp.content_type.?);
    var parsed_abort = try std.json.parseFromSlice(transactions_api.TransactionStatusResponse, std.testing.allocator, abort_resp.body, .{});
    defer parsed_abort.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_abort.value.status);
}

test "api http server serves transaction session cleanup route" {
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);
    defer server.deinit();

    var begin_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{\"sync_level\":\"write\"}",
    });
    defer begin_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), begin_resp.status);

    var cleanup_resp = try server.handle(.{
        .method = .POST,
        .uri = "/transactions/cleanup?cutoff_ns=18446744073709551615",
        .content_type = "application/json",
        .body = "",
    });
    defer cleanup_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), cleanup_resp.status);
    try std.testing.expectEqualStrings("application/json", cleanup_resp.content_type.?);
    var parsed_cleanup = try std.json.parseFromSlice(transactions_api.SessionCleanupResponse, std.testing.allocator, cleanup_resp.body, .{});
    defer parsed_cleanup.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_cleanup.value.removed);

    var list_resp = try server.handle(.{
        .method = .GET,
        .uri = routes.Routes.transactions,
        .content_type = null,
        .body = "",
    });
    defer list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("application/json", list_resp.content_type.?);
    var parsed_list = try std.json.parseFromSlice(transactions_api.SessionListResponse, std.testing.allocator, list_resp.body, .{});
    defer parsed_list.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed_list.value.session_count);
}

test "api http server reloads durable transaction sessions after restart" {
    const alloc = std.testing.allocator;
    const StoredTitle = struct {
        title: []const u8,
    };
    const path = "/tmp/antfly-api-http-session-restart";
    const session_path = "/tmp/antfly-api-http-session-restart-sessions";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 7,
    });
    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server1 = try ApiHttpServer.initWithConfig(std.testing.allocator, .{ .session_store_path = session_path }, source.iface(), null, table_source.source());
    defer server1.deinit();

    var begin_resp = try server1.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{}",
    });
    defer begin_resp.deinit(std.testing.allocator);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, std.testing.allocator, begin_resp.body, .{});
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const stage_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_write_suffix,
    });
    defer std.testing.allocator.free(stage_uri);
    var stage_resp = try server1.handle(.{
        .method = .POST,
        .uri = stage_uri,
        .content_type = "application/json",
        .body = "{\"table\":\"docs\",\"key\":\"doc:a\",\"document\":{\"title\":\"after restart\"}}",
    });
    defer stage_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), stage_resp.status);
    try std.testing.expectEqualStrings("application/json", stage_resp.content_type.?);
    var parsed_stage = try std.json.parseFromSlice(transactions_api.TransactionStatusResponse, std.testing.allocator, stage_resp.body, .{});
    defer parsed_stage.deinit();
    try std.testing.expectEqualStrings("staged", parsed_stage.value.status);

    var server2 = try ApiHttpServer.initWithConfig(std.testing.allocator, .{ .session_store_path = session_path }, source.iface(), null, table_source.source());
    defer server2.deinit();
    const commit_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_commit_suffix,
    });
    defer std.testing.allocator.free(commit_uri);
    var commit_resp = try server2.handle(.{
        .method = .POST,
        .uri = commit_uri,
        .content_type = "application/json",
        .body = "",
    });
    defer commit_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), commit_resp.status);
    try std.testing.expectEqualStrings("application/json", commit_resp.content_type.?);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.SessionCommitResponse, std.testing.allocator, commit_resp.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);
    try std.testing.expectEqualStrings(txn_id_hex, parsed_commit.value.transaction_id);

    var updated = (try db.lookup(alloc, "doc:a", .{})).?;
    defer updated.deinit(alloc);
    const stored = try std.json.parseFromSliceLeaky(StoredTitle, alloc, updated.json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    try std.testing.expectEqualStrings("after restart", stored.title);
}

test "api http server enforces configured savepoint limits and exposes remaining capacity" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-session-savepoint-limit";
    const session_path = "/tmp/antfly-api-http-session-savepoint-limit-sessions";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 7,
    });
    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_savepoint_limit = 1,
        },
        source.iface(),
        null,
        table_source.source(),
    );
    defer server.deinit();

    var begin_resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{}",
    });
    defer begin_resp.deinit(alloc);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, begin_resp.body, .{});
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const info_uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ routes.Routes.transactions_prefix, txn_id_hex });
    defer alloc.free(info_uri);
    var info_before = try server.handle(.{
        .method = .GET,
        .uri = info_uri,
    });
    defer info_before.deinit(alloc);
    var parsed_info_before = try std.json.parseFromSlice(transactions_api.SessionStatusResponse, alloc, info_before.body, .{});
    defer parsed_info_before.deinit();
    try std.testing.expectEqual(@as(?usize, 1), parsed_info_before.value.savepoint_limit);
    try std.testing.expectEqual(@as(?usize, 1), parsed_info_before.value.remaining_savepoints);

    const savepoint_uri = try std.fmt.allocPrint(alloc, "{s}{s}{s}", .{
        routes.Routes.transactions_prefix,
        txn_id_hex,
        routes.Routes.transactions_savepoints_suffix,
    });
    defer alloc.free(savepoint_uri);
    var savepoint_resp = try server.handle(.{
        .method = .POST,
        .uri = savepoint_uri,
        .body = "",
    });
    defer savepoint_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), savepoint_resp.status);

    var info_after = try server.handle(.{
        .method = .GET,
        .uri = info_uri,
    });
    defer info_after.deinit(alloc);
    var parsed_info_after = try std.json.parseFromSlice(transactions_api.SessionStatusResponse, alloc, info_after.body, .{});
    defer parsed_info_after.deinit();
    try std.testing.expectEqual(@as(?usize, 1), parsed_info_after.value.savepoint_limit);
    try std.testing.expectEqual(@as(?usize, 0), parsed_info_after.value.remaining_savepoints);

    var savepoint_again = try server.handle(.{
        .method = .POST,
        .uri = savepoint_uri,
        .body = "",
    });
    defer savepoint_again.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), savepoint_again.status);
    try std.testing.expect(std.mem.indexOf(u8, savepoint_again.body, "savepoint limit exceeded") != null);
}

test "api http server enforces session adoption timeout when configured" {
    const alloc = std.testing.allocator;
    const session_path = "/tmp/antfly-api-http-session-adopt-timeout";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    const session_path_z = try alloc.dupeZ(u8, session_path);
    defer alloc.free(session_path_z);
    var session_store = try docstore_mod.DocStore.open(alloc, session_path_z, .{});
    defer session_store.close();
    var durable = transactions_api.DurableSessionStore.init(alloc, &session_store);
    const lease_store = transactions_api.SessionLeaseStore.init(alloc, &session_store);

    var registry = transactions_api.SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{ .sync_level = .write }, 7);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeRouter = struct {
        local_node_id: u64,

        fn iface(self: *@This()) table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.local_node_id;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
            return .active;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    const FailingExecutor = struct {
        fn executor() http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return error.ConnectionResetByPeer;
        }
    };

    var router = FakeRouter{ .local_node_id = 8 };
    var fake_source = FakeSource{};
    var server = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_router = router.iface(),
            .session_executor = FailingExecutor.executor(),
            .session_owner_lease_ttl_ns = std.time.ns_per_s,
        },
        fake_source.iface(),
        null,
        null,
    );
    defer server.deinit();

    try std.testing.expect(!(try server.tryAdoptSession(session.txn_id)));
}

test "api http server renews owned session leases on request cadence" {
    const alloc = std.testing.allocator;
    const session_path = "/tmp/antfly-api-http-session-renew-cadence";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeRouter = struct {
        local_node_id: u64,

        fn iface(self: *@This()) table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.local_node_id;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
            return .active;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    var source = FakeSource{};
    var owner_router = FakeRouter{ .local_node_id = 7 };
    var owner = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_router = owner_router.iface(),
            .session_owner_lease_ttl_ns = 50 * std.time.ns_per_ms,
            .session_owner_lease_renew_interval_ns = std.time.ns_per_ms,
        },
        source.iface(),
        null,
        null,
    );
    defer owner.deinit();

    var begin_resp = try owner.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{}",
    });
    defer begin_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), begin_resp.status);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, begin_resp.body, .{});
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;
    const txn_id = try distributed_txn.parseTxnIdHex(txn_id_hex);

    std.Thread.yield() catch {};
    const info_uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ routes.Routes.transactions_prefix, txn_id_hex });
    defer alloc.free(info_uri);
    var info_resp = try owner.handle(.{
        .method = .GET,
        .uri = info_uri,
    });
    defer info_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), info_resp.status);
    var parsed_info = try std.json.parseFromSlice(transactions_api.SessionStatusResponse, alloc, info_resp.body, .{});
    defer parsed_info.deinit();
    try std.testing.expectEqualStrings("held", parsed_info.value.lease_state);

    var adopter_router = FakeRouter{ .local_node_id = 8 };
    var adopter = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_router = adopter_router.iface(),
            .session_owner_lease_ttl_ns = 50 * std.time.ns_per_ms,
        },
        source.iface(),
        null,
        null,
    );
    defer adopter.deinit();

    try std.testing.expect(!(try adopter.tryAdoptSession(txn_id)));
}

test "api http server can renew owned session leases via explicit maintenance hook" {
    const alloc = std.testing.allocator;
    const session_path = "/tmp/antfly-api-http-session-renew-maintenance";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeRouter = struct {
        local_node_id: u64,

        fn iface(self: *@This()) table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.local_node_id;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
            return .active;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    var source = FakeSource{};
    var owner_router = FakeRouter{ .local_node_id = 7 };
    var owner = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_router = owner_router.iface(),
            .session_owner_lease_ttl_ns = 50 * std.time.ns_per_ms,
            .session_owner_lease_renew_interval_ns = std.time.ns_per_ms,
        },
        source.iface(),
        null,
        null,
    );
    defer owner.deinit();

    var begin_resp = try owner.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{}",
    });
    defer begin_resp.deinit(alloc);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, begin_resp.body, .{});
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;
    const txn_id = try distributed_txn.parseTxnIdHex(txn_id_hex);

    std.Thread.yield() catch {};
    try owner.runSessionMaintenanceOnce();

    const info_uri = try std.fmt.allocPrint(alloc, "{s}{s}", .{ routes.Routes.transactions_prefix, txn_id_hex });
    defer alloc.free(info_uri);
    var info_resp = try owner.handle(.{
        .method = .GET,
        .uri = info_uri,
    });
    defer info_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), info_resp.status);
    var parsed_info = try std.json.parseFromSlice(transactions_api.SessionStatusResponse, alloc, info_resp.body, .{});
    defer parsed_info.deinit();
    try std.testing.expectEqualStrings("held", parsed_info.value.lease_state);

    var adopter_router = FakeRouter{ .local_node_id = 8 };
    var adopter = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_router = adopter_router.iface(),
            .session_owner_lease_ttl_ns = 50 * std.time.ns_per_ms,
        },
        source.iface(),
        null,
        null,
    );
    defer adopter.deinit();

    try std.testing.expect(!(try adopter.tryAdoptSession(txn_id)));
}

test "api http server runs session maintenance for internal group routes" {
    const alloc = std.testing.allocator;
    const session_path = "/tmp/antfly-api-http-session-renew-internal-route";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), session_path) catch {};

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeRouter = struct {
        local_node_id: u64,

        fn iface(self: *@This()) table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.local_node_id;
        }

        fn localStatus(_: *anyopaque, _: u64) raft_host.HostedReplicaStatus {
            return .active;
        }

        fn nodeBaseUri(_: *anyopaque, _: std.mem.Allocator, _: u64) !?[]u8 {
            return null;
        }
    };

    var source = FakeSource{};
    var owner_router = FakeRouter{ .local_node_id = 7 };
    var owner = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_router = owner_router.iface(),
            .session_owner_lease_ttl_ns = 50 * std.time.ns_per_ms,
            .session_owner_lease_renew_interval_ns = std.time.ns_per_ms,
        },
        source.iface(),
        null,
        null,
    );
    defer owner.deinit();

    var begin_resp = try owner.handle(.{
        .method = .POST,
        .uri = routes.Routes.transactions_begin,
        .content_type = "application/json",
        .body = "{}",
    });
    defer begin_resp.deinit(alloc);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, begin_resp.body, .{});
    defer parsed_begin.deinit();
    const txn_id = try distributed_txn.parseTxnIdHex(parsed_begin.value.transaction_id);

    std.Thread.yield() catch {};
    var internal_resp = (try owner.handleInternalRoute(.{
        .method = .GET,
        .uri = "/internal/v1/groups/7/db/median-key",
        .body = "",
    })).?;
    defer internal_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), internal_resp.status);

    var adopter_router = FakeRouter{ .local_node_id = 8 };
    var adopter = try ApiHttpServer.initWithConfig(
        alloc,
        .{
            .session_store_path = session_path,
            .session_router = adopter_router.iface(),
            .session_owner_lease_ttl_ns = 50 * std.time.ns_per_ms,
        },
        source.iface(),
        null,
        null,
    );
    defer adopter.deinit();

    try std.testing.expect(!(try adopter.tryAdoptSession(txn_id)));
}

test "api http server handleInternalRoute matches handle for internal group lookups" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface() StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .lookup_group_local = lookupGroupLocal,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            return null;
        }

        fn lookupGroupLocal(
            _: *anyopaque,
            inner_alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:a", key);
            try std.testing.expectEqual(@as(?usize, 1), opts.fields.len);
            try std.testing.expectEqualStrings("title", opts.fields[0]);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            return .{
                .json = try inner_alloc.dupe(u8, "{\"title\":\"alpha\"}"),
                .version = 42,
            };
        }
    };

    var server = ApiHttpServer.init(alloc, .{}, FakeSource.iface(), FakeReads.source(), null);

    const req: http_common.HttpRequest = .{
        .method = .GET,
        .uri = "/internal/v1/groups/7/tables/docs/lookup/doc:a?fields=title",
    };

    var via_handle = try server.handle(req);
    defer via_handle.deinit(alloc);
    var via_internal = (try server.handleInternalRoute(req)).?;
    defer via_internal.deinit(alloc);

    try std.testing.expectEqual(via_handle.status, via_internal.status);
    try std.testing.expectEqualStrings(via_handle.content_type.?, via_internal.content_type.?);
    try std.testing.expectEqualStrings(via_handle.body, via_internal.body);
    try std.testing.expectEqual(@as(usize, 1), via_handle.headers.len);
    try std.testing.expectEqual(@as(usize, 1), via_internal.headers.len);
    try std.testing.expectEqualStrings(via_handle.headers[0].name, via_internal.headers[0].name);
    try std.testing.expectEqualStrings(via_handle.headers[0].value, via_internal.headers[0].value);
}

test "api http server maps public query doc identity mismatch to unavailable" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface() StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    const FakeReads = struct {
        fn source() table_reads.TableReadSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.LookupOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: db_mod.types.ScanOptions,
            _: raft_mod.ReadConsistency,
        ) !?table_reads.ScanResponse {
            return null;
        }

        fn query(
            _: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            _: db_mod.types.SearchRequest,
            _: raft_mod.ReadConsistency,
        ) !?query_api.QueryResponse {
            try std.testing.expectEqualStrings("docs", table_name);
            return error.DocIdentityNamespaceMismatch;
        }
    };

    var server = ApiHttpServer.init(alloc, .{}, FakeSource.iface(), FakeReads.source(), null);
    defer server.deinit();

    var resp = try server.handlePublicTableQuery("docs",
        \\{"query":{"match_all":{}}}
    , null);
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("doc identity unavailable", resp.body);
}

test "api http server serves internal group transaction routes" {
    const alloc = std.testing.allocator;
    const StoredTitle = struct {
        title: []const u8,
    };
    const path = "/tmp/antfly-api-http-txn";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, table_source.source());
    defer server.deinit();
    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");

    const begin_body = try distributed_txn.encodeTxnBeginRequest(std.testing.allocator, .{
        .txn_id = txn_id,
        .begin_timestamp = 10_000,
        .participants = &.{"group:7"},
    });
    defer std.testing.allocator.free(begin_body);
    var begin_resp = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/txn-begin",
        .content_type = "application/json",
        .body = begin_body,
    });
    defer begin_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), begin_resp.status);

    const prepare_body = try distributed_txn.encodeTxnPrepareRequest(std.testing.allocator, .{
        .txn_id = txn_id,
        .req = .{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        },
    });
    defer std.testing.allocator.free(prepare_body);
    var prepare_resp = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/txn-prepare",
        .content_type = "application/json",
        .body = prepare_body,
    });
    defer prepare_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), prepare_resp.status);

    const status_body = try distributed_txn.encodeTxnStatusRequest(std.testing.allocator, txn_id);
    defer std.testing.allocator.free(status_body);
    var pending_resp = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/txn-status",
        .content_type = "application/json",
        .body = status_body,
    });
    defer pending_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), pending_resp.status);
    const pending = try distributed_txn.parseTxnStatusResponse(std.testing.allocator, pending_resp.body);
    try std.testing.expectEqual(db_mod.types.TxnStatus.pending, pending.status);

    const resolve_body = try distributed_txn.encodeTxnResolveRequest(std.testing.allocator, .{
        .txn_id = txn_id,
        .status = .committed,
        .commit_version = 10_001,
    });
    defer std.testing.allocator.free(resolve_body);
    var resolve_resp = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/txn-resolve",
        .content_type = "application/json",
        .body = resolve_body,
    });
    defer resolve_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resolve_resp.status);

    var committed_resp = try server.handle(.{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/txn-status",
        .content_type = "application/json",
        .body = status_body,
    });
    defer committed_resp.deinit(std.testing.allocator);
    const committed = try distributed_txn.parseTxnStatusResponse(std.testing.allocator, committed_resp.body);
    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, committed.status);

    var lookup = (try db.lookup(alloc, "doc:a", .{})).?;
    defer lookup.deinit(alloc);
    const stored = try std.json.parseFromSliceLeaky(StoredTitle, alloc, lookup.json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    try std.testing.expectEqualStrings("alpha", stored.title);
}

test "api http server serves table metadata list and detail" {
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_tables = 2, .projected_ranges = 2, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 1, .name = "docs", .placement_role = "data" },
                    .{ .table_id = 2, .name = "logs", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:z" },
                    .{ .group_id = 20, .table_id = 2, .start_key = "log:a", .end_key = "log:z" },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);

    var list_resp = try server.handle(.{ .method = .GET, .uri = "/tables?prefix=do" });
    defer list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), list_resp.status);
    try std.testing.expectEqualStrings("application/json", list_resp.content_type.?);
    var parsed_list = try std.json.parseFromSlice([]metadata_openapi.TableStatus, std.testing.allocator, list_resp.body, .{});
    defer parsed_list.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_list.value.len);
    try std.testing.expectEqualStrings("docs", parsed_list.value[0].name);

    var detail_resp = try server.handle(.{ .method = .GET, .uri = "/tables/docs" });
    defer detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), detail_resp.status);
    try std.testing.expectEqualStrings("application/json", detail_resp.content_type.?);
    var parsed_detail = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, detail_resp.body, .{});
    defer parsed_detail.deinit();
    try std.testing.expectEqualStrings("docs", parsed_detail.value.name);
    try std.testing.expectEqual(@as(usize, 1), parsed_detail.value.shards.map.count());
}

test "api http server serves runtime schema debug on table and index detail" {
    const RuntimeSchemaDebugTableResponse = struct {
        const Binding = struct {
            index_name: []const u8,
            schema_slot: ?[]const u8 = null,
        };

        const SchemaEntry = struct {
            slot: []const u8,
            status: []const u8,
            runtime_schema: ?std.json.Value = null,
        };
        const AlgebraicCapability = struct {
            slot: []const u8,
            status: []const u8,
            group_field_count: u32 = 0,
            measure_field_count: u32 = 0,
            time_field_count: u32 = 0,
            config: ?std.json.Value = null,
        };

        debug: struct {
            runtime_schemas: []const SchemaEntry,
            full_text_index_bindings: []const Binding,
            algebraic_capabilities: []const AlgebraicCapability,
        },
    };
    const RuntimeSchemaDebugIndexResponse = struct {
        debug: struct {
            binding: struct {
                index_name: []const u8,
                schema_slot: ?[]const u8 = null,
                runtime_schema: ?std.json.Value = null,
            },
        },
    };
    const Helpers = struct {
        fn hasAnalyzer(value: std.json.Value, expected: []const u8) bool {
            return switch (value) {
                .object => |object| blk: {
                    if (object.get("analyzer")) |analyzer| {
                        if (analyzer == .string and std.mem.eql(u8, analyzer.string, expected)) break :blk true;
                    }
                    var it = object.iterator();
                    while (it.next()) |entry| {
                        if (hasAnalyzer(entry.value_ptr.*, expected)) break :blk true;
                    }
                    break :blk false;
                },
                .array => |items| blk: {
                    for (items.items) |item| {
                        if (hasAnalyzer(item, expected)) break :blk true;
                    }
                    break :blk false;
                },
                else => false,
            };
        }
    };
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_tables = 1, .projected_ranges = 1, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .schema_json = "{\"version\":1,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\",\"x-antfly-types\":[\"text\"],\"x-antfly-analyzer\":\"french\"}}}}}}",
                    .read_schema_json = "{\"version\":0,\"default_type\":\"doc\",\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\",\"x-antfly-types\":[\"search_as_you_type\"]}}}}}}",
                    .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 1, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);

    var table_resp = try server.handle(.{ .method = .GET, .uri = "/tables/docs?debug=runtime_schema" });
    defer table_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), table_resp.status);
    try std.testing.expectEqualStrings("application/json", table_resp.content_type.?);
    var parsed_table_contract = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, table_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_table_contract.deinit();
    try std.testing.expectEqualStrings("docs", parsed_table_contract.value.name);
    var parsed_table = try std.json.parseFromSlice(RuntimeSchemaDebugTableResponse, std.testing.allocator, table_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_table.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_table.value.debug.runtime_schemas.len);
    try std.testing.expectEqualStrings("active", parsed_table.value.debug.runtime_schemas[0].slot);
    try std.testing.expectEqualStrings("read", parsed_table.value.debug.runtime_schemas[1].slot);
    try std.testing.expectEqualStrings("ok", parsed_table.value.debug.runtime_schemas[0].status);
    try std.testing.expectEqualStrings("ok", parsed_table.value.debug.runtime_schemas[1].status);
    try std.testing.expectEqual(@as(usize, 2), parsed_table.value.debug.full_text_index_bindings.len);
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_table.value.debug.full_text_index_bindings[0].index_name);
    try std.testing.expectEqualStrings("read", parsed_table.value.debug.full_text_index_bindings[0].schema_slot.?);
    try std.testing.expectEqual(@as(usize, 2), parsed_table.value.debug.algebraic_capabilities.len);
    try std.testing.expectEqualStrings("active", parsed_table.value.debug.algebraic_capabilities[0].slot);
    try std.testing.expectEqualStrings("ok", parsed_table.value.debug.algebraic_capabilities[0].status);
    try std.testing.expect(parsed_table.value.debug.algebraic_capabilities[0].group_field_count > 0);
    try std.testing.expect(parsed_table.value.debug.algebraic_capabilities[0].config != null);
    try std.testing.expect(parsed_table.value.debug.runtime_schemas[0].runtime_schema != null);
    try std.testing.expect(Helpers.hasAnalyzer(parsed_table.value.debug.runtime_schemas[0].runtime_schema.?, "french"));

    var index_resp = try server.handle(.{ .method = .GET, .uri = "/tables/docs/indexes/full_text_index_v0?debug=runtime_schema" });
    defer index_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), index_resp.status);
    try std.testing.expectEqualStrings("application/json", index_resp.content_type.?);
    var parsed_index_contract = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, index_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_index_contract.deinit();
    try std.testing.expect(parsed_index_contract.value.object.get("debug") != null);
    var parsed_index = try std.json.parseFromSlice(RuntimeSchemaDebugIndexResponse, std.testing.allocator, index_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_index.deinit();
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_index.value.debug.binding.index_name);
    try std.testing.expectEqualStrings("read", parsed_index.value.debug.binding.schema_slot.?);
    try std.testing.expect(parsed_index.value.debug.binding.runtime_schema != null);
    try std.testing.expect(Helpers.hasAnalyzer(parsed_index.value.debug.binding.runtime_schema.?, "search_as_you_type_index_prefix"));
}

test "api http server serves table index metadata routes" {
    const FakeSource = struct {
        admin_snapshot_calls: usize = 0,
        cached_snapshot_calls: usize = 0,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.admin_snapshot_calls += 1;
            return snapshot();
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cached_snapshot_calls += 1;
            return snapshot();
        }

        fn snapshot() metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384},\"alg\":{\"type\":\"algebraic\"}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);

    var list_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes",
    });
    defer list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), list_resp.status);
    try std.testing.expectEqualStrings("application/json", list_resp.content_type.?);
    var parsed_list = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, list_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_list.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed_list.value.array.items.len);
    try std.testing.expectEqualStrings("search_idx", parsed_list.value.array.items[0].object.get("config").?.object.get("name").?.string);
    try std.testing.expectEqualStrings("embed_idx", parsed_list.value.array.items[1].object.get("config").?.object.get("name").?.string);
    try std.testing.expectEqualStrings("alg", parsed_list.value.array.items[2].object.get("config").?.object.get("name").?.string);

    var detail_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/embed_idx",
    });
    defer detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), detail_resp.status);
    try std.testing.expectEqualStrings("application/json", detail_resp.content_type.?);
    var parsed_detail = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, detail_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_detail.deinit();
    try std.testing.expectEqualStrings("embed_idx", parsed_detail.value.object.get("config").?.object.get("name").?.string);
    try std.testing.expectEqual(@as(usize, 2), source.cached_snapshot_calls);
    try std.testing.expectEqual(@as(usize, 0), source.admin_snapshot_calls);

    var algebraic_detail_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/alg",
    });
    defer algebraic_detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), algebraic_detail_resp.status);
    try std.testing.expectEqualStrings("application/json", algebraic_detail_resp.content_type.?);
    var parsed_algebraic_detail = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, algebraic_detail_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_algebraic_detail.deinit();
    try std.testing.expectEqualStrings("alg", parsed_algebraic_detail.value.object.get("config").?.object.get("name").?.string);
    try std.testing.expectEqualStrings("algebraic", parsed_algebraic_detail.value.object.get("config").?.object.get("type").?.string);
    try std.testing.expect(parsed_algebraic_detail.value.object.get("status") != null);

    var algebraic_child_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/alg/algebraic",
    });
    defer algebraic_child_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), algebraic_child_resp.status);
}

test "api http server index status is cache only" {
    const FakeSource = struct {
        admin_snapshot_calls: usize = 0,
        cached_snapshot_calls: usize = 0,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.admin_snapshot_calls += 1;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cached_snapshot_calls += 1;
            return null;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);

    var detail_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/embed_idx",
    });
    defer detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), detail_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.cached_snapshot_calls);
    try std.testing.expectEqual(@as(usize, 0), source.admin_snapshot_calls);
}

test "api http server reports table storage empty from read visibility" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-table-storage-empty";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }
    try db.updateRange(.{ .start = "", .end = "" });

    var read_source = table_reads.BoundTableReadSource.init("docs", 7, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = tables_api.default_indexes_json,
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7,
                    .table_id = 1,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), read_source.source(), null);

    var empty_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs",
    });
    defer empty_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), empty_resp.status);
    try std.testing.expectEqualStrings("application/json", empty_resp.content_type.?);
    var parsed_empty = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, empty_resp.body, .{});
    defer parsed_empty.deinit();
    try std.testing.expectEqual(@as(?i64, 0), parsed_empty.value.storage_status.disk_usage);
    try std.testing.expectEqual(@as(?bool, true), parsed_empty.value.storage_status.empty);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var non_empty_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs",
    });
    defer non_empty_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), non_empty_resp.status);
    try std.testing.expectEqualStrings("application/json", non_empty_resp.content_type.?);
    var parsed_non_empty = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, non_empty_resp.body, .{});
    defer parsed_non_empty.deinit();
    try std.testing.expectEqual(@as(?i64, 0), parsed_non_empty.value.storage_status.disk_usage);
    try std.testing.expectEqual(@as(?bool, false), parsed_non_empty.value.storage_status.empty);

    var list_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables",
    });
    defer list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), list_resp.status);
    try std.testing.expectEqualStrings("application/json", list_resp.content_type.?);
    var parsed_list = try std.json.parseFromSlice([]metadata_openapi.TableStatus, std.testing.allocator, list_resp.body, .{});
    defer parsed_list.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_list.value.len);
    try std.testing.expectEqual(@as(?i64, 0), parsed_list.value[0].storage_status.disk_usage);
    try std.testing.expectEqual(@as(?bool, false), parsed_list.value[0].storage_status.empty);
}

test "api http server table status uses runtime stats without probing storage" {
    const alloc = std.testing.allocator;

    const TableStatusResponse = struct {
        storage_status: struct {
            empty: ?bool = null,
        },
    };

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = tables_api.default_indexes_json,
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7,
                    .table_id = 1,
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

    const FakeReads = struct {
        scan_calls: std.atomic.Value(u32) = .init(0),
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.scan_calls.fetchAdd(1, .monotonic);
            return null;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return null;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            _ = self.status_calls.fetchAdd(1, .monotonic);
            const items = try allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
            items[0] = .{
                .group_id = 7,
                .stats = .{
                    .doc_count = 5,
                },
            };
            return .{ .items = items };
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), null);

    var resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u32, 1), reads.status_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), reads.scan_calls.load(.monotonic));
    var parsed = try std.json.parseFromSlice(TableStatusResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?bool, false), parsed.value.storage_status.empty);
}

test "api http server serves local index runtime backfill status" {
    const alloc = std.testing.allocator;
    const LocalIndexStatusResponse = struct {
        const Stats = struct {
            rebuilding: ?bool = null,
            backfill_active: ?bool = null,
            doc_count: ?u64 = null,
            total_indexed: ?u64 = null,
        };

        config: std.json.Value,
        status: Stats,
        shard_status: struct {
            @"7": ?Stats = null,
            local: ?Stats = null,
        },
    };
    const path = "/tmp/antfly-api-http-index-status";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer {
        db.close();
        std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    }

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
    try db.addIndex(.{
        .name = "search_idx",
        .kind = .full_text,
        .config_json = "{}",
    });

    const index_root = try std.fmt.allocPrint(alloc, "{s}/indexes/search_idx", .{path});
    defer alloc.free(index_root);
    const rebuild_state = db_mod.backfill_state.RebuildState.init(index_root);
    try rebuild_state.update("doc:a");

    var read_source = table_reads.BoundTableReadSource.init("docs", 7, &db, raft_mod.read_gate.noopReadableLeaseRequester());

    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 7,
                    .table_id = 1,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), read_source.source(), null);

    var detail_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/search_idx",
    });
    defer detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), detail_resp.status);
    try std.testing.expectEqualStrings("application/json", detail_resp.content_type.?);
    var parsed_contract_detail = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, detail_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_contract_detail.deinit();
    try std.testing.expectEqualStrings("search_idx", parsed_contract_detail.value.object.get("config").?.object.get("name").?.string);
    var parsed_detail = try std.json.parseFromSlice(LocalIndexStatusResponse, std.testing.allocator, detail_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_detail.deinit();
    try std.testing.expectEqualStrings("search_idx", parsed_detail.value.config.object.get("name").?.string);
    try std.testing.expectEqual(@as(?bool, false), parsed_detail.value.status.backfill_active);
    try std.testing.expectEqual(@as(?bool, false), parsed_detail.value.status.rebuilding);
    try std.testing.expectEqual(@as(?u64, 1), parsed_detail.value.status.doc_count);
    try std.testing.expectEqual(@as(?u64, 1), parsed_detail.value.status.total_indexed);
    const local_shard = parsed_detail.value.shard_status.@"7" orelse parsed_detail.value.shard_status.local.?;
    try std.testing.expectEqual(@as(?bool, false), local_shard.backfill_active);
    try std.testing.expectEqual(@as(?u64, 1), local_shard.doc_count);
}

test "api http server create index relies on metadata projection without local index polling" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        projection_wait_calls: std.atomic.Value(u32) = .init(0),
        indexes_json: []const u8 = tables_api.default_indexes_json,
        owns_indexes_json: bool = false,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .create_index = createIndex,
                    .wait_table_projection = waitTableProjection,
                },
            };
        }

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.owns_indexes_json) allocator.free(self.indexes_json);
        }

        fn replaceIndexesJson(self: *@This(), allocator: std.mem.Allocator, next: []const u8, owns_next: bool) void {
            if (self.owns_indexes_json) allocator.free(self.indexes_json);
            self.indexes_json = next;
            self.owns_indexes_json = owns_next;
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .indexes_json = self.indexes_json,
                    .placement_role = "data",
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

        fn createIndex(ptr: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            const next = try indexes_api.addIndexToTableIndexesJson(allocator, self.indexes_json, index_name, index_json);
            self.replaceIndexesJson(allocator, next, true);
        }

        fn waitTableProjection(ptr: *anyopaque, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(schema_json == null);
            try std.testing.expect(indexes_json != null);
            _ = self.projection_wait_calls.fetchAdd(1, .monotonic);
        }
    };

    const FakeWrites = struct {
        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .create_index = createIndex,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn createIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) anyerror!?void {}
    };

    var source = FakeSource{};
    defer source.deinit(alloc);
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(
        alloc,
        .{},
        source.iface(),
        null,
        writes.source(),
    );

    const create_index_body = try test_contract_helpers.encodeCreateIndexRequest(alloc, "embed_idx");
    defer alloc.free(create_index_body);
    var create_index_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/indexes/embed_idx",
        .content_type = "application/json",
        .body = create_index_body,
    });
    defer create_index_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), create_index_resp.status);
    try std.testing.expectEqual(@as(u32, 1), source.projection_wait_calls.load(.monotonic));
}

test "api http server create index expands schema-derived algebraic config" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        const schema_json =
            \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"customer":{"type":"keyword"},"amount":{"type":"number"},"created_at":{"type":"datetime"}}}}}}
        ;

        projection_wait_calls: std.atomic.Value(u32) = .init(0),
        indexes_json: []const u8 = tables_api.default_indexes_json,
        owns_indexes_json: bool = false,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .create_index = createIndex,
                    .wait_table_projection = waitTableProjection,
                },
            };
        }

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            if (self.owns_indexes_json) allocator.free(self.indexes_json);
        }

        fn replaceIndexesJson(self: *@This(), allocator: std.mem.Allocator, next: []const u8, owns_next: bool) void {
            if (self.owns_indexes_json) allocator.free(self.indexes_json);
            self.indexes_json = next;
            self.owns_indexes_json = owns_next;
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .schema_json = schema_json,
                    .indexes_json = self.indexes_json,
                    .placement_role = "data",
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

        fn createIndex(ptr: *anyopaque, allocator: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(std.mem.indexOf(u8, index_json, "\"derive_from_schema\"") == null);
            try std.testing.expect(std.mem.indexOf(u8, index_json, "\"group_fields\"") != null);
            try std.testing.expect(std.mem.indexOf(u8, index_json, "\"measure_fields\"") != null);
            const next = try indexes_api.addIndexToTableIndexesJson(allocator, self.indexes_json, index_name, index_json);
            self.replaceIndexesJson(allocator, next, true);
        }

        fn waitTableProjection(ptr: *anyopaque, table_name: []const u8, schema_json_opt: ?[]const u8, indexes_json: ?[]const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(schema_json_opt == null);
            try std.testing.expect(indexes_json != null);
            try std.testing.expectEqualStrings(self.indexes_json, indexes_json.?);
            _ = self.projection_wait_calls.fetchAdd(1, .monotonic);
        }
    };

    const FakeWrites = struct {
        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .create_index = createIndex,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn createIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, index_json: []const u8) anyerror!?void {
            try std.testing.expect(std.mem.indexOf(u8, index_json, "\"derive_from_schema\"") == null);
            try std.testing.expect(std.mem.indexOf(u8, index_json, "\"materializations\"") != null);
        }
    };

    var source = FakeSource{};
    defer source.deinit(alloc);
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(
        alloc,
        .{},
        source.iface(),
        null,
        writes.source(),
    );

    const create_index_body =
        \\{"name":"sales_rollup","type":"algebraic","derive_from_schema":true}
    ;
    var create_index_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/indexes/sales_rollup",
        .content_type = "application/json",
        .body = create_index_body,
    });
    defer create_index_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 201), create_index_resp.status);
    try std.testing.expectEqual(@as(u32, 1), source.projection_wait_calls.load(.monotonic));
    try std.testing.expect(std.mem.indexOf(u8, source.indexes_json, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, source.indexes_json, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, source.indexes_json, "\"measure_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, source.indexes_json, "\"materializations\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, source.indexes_json, "\"sum_by_customer\"") == null);
}

test "api http server rejects public algebraic materialization config" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        create_index_calls: std.atomic.Value(u32) = .init(0),
        projection_wait_calls: std.atomic.Value(u32) = .init(0),

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .create_index = createIndex,
                    .wait_table_projection = waitTableProjection,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .schema_json = "{\"version\":1}",
                    .indexes_json = tables_api.default_indexes_json,
                    .placement_role = "data",
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

        fn createIndex(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.create_index_calls.fetchAdd(1, .monotonic);
        }

        fn waitTableProjection(ptr: *anyopaque, _: []const u8, _: ?[]const u8, _: ?[]const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.projection_wait_calls.fetchAdd(1, .monotonic);
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(
        alloc,
        .{},
        source.iface(),
        null,
        null,
    );

    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/indexes/manual_alg",
        .content_type = "application/json",
        .body = "{\"name\":\"manual_alg\",\"type\":\"algebraic\",\"derive_from_schema\":true,\"materializations\":[]}",
    });
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqual(@as(u32, 0), source.create_index_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), source.projection_wait_calls.load(.monotonic));
}

test "api http server serves provisioned index runtime backfill status across shards" {
    const alloc = std.testing.allocator;
    const ProvisionedIndexStatusResponse = struct {
        const Stats = struct {
            rebuilding: ?bool = null,
            backfill_active: ?bool = null,
            doc_count: ?u64 = null,
            total_indexed: ?u64 = null,
        };

        config: std.json.Value,
        status: Stats,
        shard_status: struct {
            @"7001": ?Stats = null,
            @"7002": ?Stats = null,
            local: ?Stats = null,
        },
    };
    const path = "/tmp/antfly-api-http-provisioned-index-status";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    const left_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7001);
    defer alloc.free(left_path);
    const right_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, path, 7002);
    defer alloc.free(right_path);

    var left_db = try db_mod.DB.open(alloc, left_path, .{});
    defer left_db.close();
    var right_db = try db_mod.DB.open(alloc, right_path, .{});
    defer right_db.close();
    try left_db.updateRange(.{ .start = "", .end = "doc:m" });
    try right_db.updateRange(.{ .start = "doc:m", .end = "" });

    try left_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
    try right_db.batch(.{
        .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" }},
    });
    try left_db.addIndex(.{
        .name = "search_idx",
        .kind = .full_text,
        .config_json = "{}",
    });
    try right_db.addIndex(.{
        .name = "search_idx",
        .kind = .full_text,
        .config_json = "{}",
    });

    const left_index_root = try std.fmt.allocPrint(alloc, "{s}/indexes/search_idx", .{left_path});
    defer alloc.free(left_index_root);
    const left_rebuild_state = db_mod.backfill_state.RebuildState.init(left_index_root);
    try left_rebuild_state.update("doc:h");
    const right_index_root = try std.fmt.allocPrint(alloc, "{s}/indexes/search_idx", .{right_path});
    defer alloc.free(right_index_root);
    const right_rebuild_state = db_mod.backfill_state.RebuildState.init(right_index_root);
    try right_rebuild_state.update("doc:z");
    const FakeCatalog = struct {
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
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeSource = struct {
        fn iface() StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .indexes_json = "{\"search_idx\":{\"type\":\"full_text\"}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var read_source = table_reads.ProvisionedTableReadSource.init(path, FakeCatalog.iface(), raft_mod.read_gate.noopReadableLeaseRequester());
    var server = ApiHttpServer.init(std.testing.allocator, .{}, FakeSource.iface(), read_source.source(), null);

    var detail_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/search_idx",
    });
    defer detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), detail_resp.status);
    try std.testing.expectEqualStrings("application/json", detail_resp.content_type.?);
    var parsed_contract_detail = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, detail_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_contract_detail.deinit();
    try std.testing.expectEqualStrings("search_idx", parsed_contract_detail.value.object.get("config").?.object.get("name").?.string);
    var parsed_detail = try std.json.parseFromSlice(ProvisionedIndexStatusResponse, std.testing.allocator, detail_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_detail.deinit();
    try std.testing.expectEqualStrings("search_idx", parsed_detail.value.config.object.get("name").?.string);
    // Provisioned runtime status reopens shard DBs, so pending backfill is resumed before stats are read.
    if (parsed_detail.value.status.backfill_active) |active| try std.testing.expect(active);
    if (parsed_detail.value.status.rebuilding) |rebuilding| try std.testing.expect(rebuilding);
    if (parsed_detail.value.status.doc_count) |doc_count| try std.testing.expect(doc_count >= 1);
    if (parsed_detail.value.status.total_indexed) |total_indexed| try std.testing.expect(total_indexed >= 1);
    if (parsed_detail.value.shard_status.@"7001" orelse parsed_detail.value.shard_status.local) |provisioned_left_shard| {
        try std.testing.expectEqual(@as(?bool, true), provisioned_left_shard.backfill_active);
        try std.testing.expectEqual(@as(?u64, 1), provisioned_left_shard.doc_count);
    }
    if (parsed_detail.value.shard_status.@"7002") |right_shard| {
        try std.testing.expectEqual(@as(?bool, true), right_shard.backfill_active);
    }
    if (parsed_detail.value.shard_status.@"7002") |right_shard| {
        try std.testing.expectEqual(@as(?u64, 1), right_shard.doc_count);
    }
}

test "api http server serves table create and drop" {
    const FakeSource = struct {
        const default_indexes_json = "{\"full_text_index_v0\":{}}";

        created: bool = false,
        projection_wait_calls: std.atomic.Value(u32) = .init(0),
        indexes_json: []const u8,
        owns_indexes_json: bool = false,
        table_record: metadata_table_manager.TableRecord = .{
            .table_id = 1,
            .name = "docs",
            .description = "docs table",
            .schema_json = "{\"kind\":\"demo\"}",
            .indexes_json = default_indexes_json,
            .replication_sources_json = "[]",
            .placement_role = "data",
        },
        range_record: metadata_table_manager.RangeRecord = .{
            .group_id = 10,
            .table_id = 1,
            .start_key = "",
            .end_key = null,
        },
        empty_tables: [0]metadata_table_manager.TableRecord = .{},
        empty_ranges: [0]metadata_table_manager.RangeRecord = .{},
        empty_stores: [0]metadata_table_manager.StoreRecord = .{},
        empty_placements: [0]raft_reconciler.PlacementIntent = .{},
        empty_splits: [0]metadata_transition_state.SplitTransitionRecord = .{},
        empty_merges: [0]metadata_transition_state.MergeTransitionRecord = .{},

        pub fn init() @This() {
            return .{
                .indexes_json = default_indexes_json,
            };
        }

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.owns_indexes_json) alloc.free(self.indexes_json);
        }

        fn replaceIndexesJson(self: *@This(), alloc: std.mem.Allocator, next: []const u8, owns_next: bool) void {
            if (self.owns_indexes_json) alloc.free(self.indexes_json);
            self.indexes_json = next;
            self.owns_indexes_json = owns_next;
            self.table_record.indexes_json = self.indexes_json;
        }

        fn tableSlice(self: *@This()) []metadata_table_manager.TableRecord {
            return @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table_record))[0..1];
        }

        fn rangeSlice(self: *@This()) []metadata_table_manager.RangeRecord {
            return @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.range_record))[0..1];
        }

        fn iface(self: *@This()) StatusSource {
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
                    .wait_table_projection = waitTableProjection,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.table_record.indexes_json = self.indexes_json;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = if (self.created)
                    @constCast(self.tableSlice())
                else
                    @constCast(self.empty_tables[0..]),
                .ranges = if (self.created)
                    @constCast(self.rangeSlice())
                else
                    @constCast(self.empty_ranges[0..]),
                .stores = @constCast(self.empty_stores[0..]),
                .placement_intents = @constCast(self.empty_placements[0..]),
                .split_transitions = @constCast(self.empty_splits[0..]),
                .merge_transitions = @constCast(self.empty_merges[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn createTable(ptr: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8, req: tables_api.CreateTableRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(?u32, 1), req.num_shards);
            try std.testing.expectEqualStrings("docs table", req.description.?);
            try std.testing.expect(req.schema_json == null);
            try std.testing.expect(try indexes_api.equivalentIndexConfigJson(std.testing.allocator, tables_api.default_indexes_json, req.indexes_json.?));
            try std.testing.expect(req.replication_sources_json == null);
            self.created = true;
            self.replaceIndexesJson(inner_alloc, try inner_alloc.dupe(u8, req.indexes_json.?), true);
        }

        fn dropTable(ptr: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.replaceIndexesJson(inner_alloc, default_indexes_json, false);
            self.created = false;
        }

        fn updateSchema(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(std.mem.indexOf(u8, schema_json, "\"document_schemas\"") != null);
            self.created = true;
            self.table_record.schema_json = schema_json;
        }

        fn createIndex(ptr: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            const next = try indexes_api.addIndexToTableIndexesJson(inner_alloc, self.indexes_json, index_name, index_json);
            self.replaceIndexesJson(inner_alloc, next, true);
        }

        fn dropIndex(ptr: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            const next = (try indexes_api.removeIndexFromTableIndexesJson(inner_alloc, self.indexes_json, index_name)) orelse return error.IndexNotFound;
            self.replaceIndexesJson(inner_alloc, next, true);
        }

        fn waitTableProjection(ptr: *anyopaque, table_name: []const u8, schema_json: ?[]const u8, indexes_json: ?[]const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            _ = self.projection_wait_calls.fetchAdd(1, .monotonic);
            if (schema_json) |expected| {
                try std.testing.expect(std.mem.eql(u8, self.table_record.schema_json, expected));
            }
            if (indexes_json) |expected| {
                try std.testing.expect(try indexes_api.equivalentIndexConfigJson(std.testing.allocator, self.indexes_json, expected));
            }
        }
    };

    var source = FakeSource.init();
    defer source.deinit(std.testing.allocator);
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var create_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs",
        .content_type = "application/json",
        .body = create_body,
    });
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), create_resp.status);
    try std.testing.expectEqualStrings("application/json", create_resp.content_type.?);
    var parsed_create = try std.json.parseFromSlice(metadata_openapi.Table, std.testing.allocator, create_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_create.deinit();
    try std.testing.expectEqualStrings("docs", parsed_create.value.name);
    try std.testing.expectEqualStrings("docs table", parsed_create.value.description.?);

    var drop_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/tables/docs",
    });
    defer drop_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 204), drop_resp.status);

    const update_schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(std.testing.allocator);
    defer std.testing.allocator.free(update_schema_body);
    var update_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/tables/docs/schema",
        .content_type = "application/json",
        .body = update_schema_body,
    });
    defer update_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), update_resp.status);
    try std.testing.expectEqualStrings("application/json", update_resp.content_type.?);
    var parsed_update = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, update_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("docs", parsed_update.value.object.get("name").?.string);
    try std.testing.expect(parsed_update.value.object.get("schema") != null);

    const create_index_body = try test_contract_helpers.encodeCreateIndexRequest(std.testing.allocator, "embed_idx");
    defer std.testing.allocator.free(create_index_body);
    var create_index_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/indexes/embed_idx",
        .content_type = "application/json",
        .body = create_index_body,
    });
    defer create_index_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), create_index_resp.status);
    try std.testing.expectEqualStrings("application/json", create_index_resp.content_type.?);
    try std.testing.expectEqualStrings("{}", create_index_resp.body);
    try std.testing.expect(std.mem.indexOf(u8, source.indexes_json, "\"embed_idx\"") != null);

    var detail_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/embed_idx",
    });
    defer detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), detail_resp.status);
    try std.testing.expectEqualStrings("application/json", detail_resp.content_type.?);
    var parsed_index = try std.json.parseFromSlice(struct {
        config: struct {
            name: []const u8,
        },
    }, std.testing.allocator, detail_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_index.deinit();
    try std.testing.expectEqualStrings("embed_idx", parsed_index.value.config.name);

    var drop_index_resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/tables/docs/indexes/embed_idx",
    });
    defer drop_index_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), drop_index_resp.status);
    try std.testing.expectEqualStrings("application/json", drop_index_resp.content_type.?);
    try std.testing.expectEqualStrings("{}", drop_index_resp.body);
    try std.testing.expectEqual(@as(u32, 2), source.projection_wait_calls.load(.monotonic));
}

test "api http server table visibility helper prefers metadata lifecycle wait" {
    const FakeSource = struct {
        lifecycle_wait_calls: std.atomic.Value(u32) = .init(0),

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .wait_table_lifecycle = waitTableLifecycle,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn waitTableLifecycle(ptr: *anyopaque, table_name: []const u8, expected: TableVisibility) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(TableVisibility.present, expected);
            _ = self.lifecycle_wait_calls.fetchAdd(1, .monotonic);
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, source.iface(), null, null);

    try server.waitForTableVisibility("docs", .present);
    try std.testing.expectEqual(@as(u32, 1), source.lifecycle_wait_calls.load(.monotonic));
}

test "api http server create table with local writes waits for projected presence without lifecycle" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        created: bool = false,
        lifecycle_wait_calls: std.atomic.Value(u32) = .init(0),

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .create_table = createTable,
                    .wait_table_lifecycle = waitTableLifecycle,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = if (self.created)
                    @constCast((&[_]metadata_table_manager.TableRecord{.{
                        .table_id = 1,
                        .name = "docs",
                        .description = "docs table",
                        .indexes_json = tables_api.default_indexes_json,
                        .placement_role = "data",
                    }})[0..])
                else
                    @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = if (self.created)
                    @constCast((&[_]metadata_table_manager.RangeRecord{.{
                        .group_id = 10,
                        .table_id = 1,
                        .start_key = "",
                        .end_key = null,
                    }})[0..])
                else
                    @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = if (self.created)
                    @constCast((&[_]metadata_table_manager.StoreRecord{.{
                        .store_id = 20,
                        .node_id = 30,
                        .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{.{
                            .group_id = 10,
                            .local_voter = true,
                        }})[0..]),
                    }})[0..])
                else
                    @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = if (self.created)
                    @constCast((&[_]raft_reconciler.PlacementIntent{.{
                        .record = .{
                            .group_id = 10,
                            .replica_id = 1,
                            .local_node_id = 30,
                        },
                        .store_id = 20,
                    }})[0..])
                else
                    @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .merged_group_statuses = if (self.created)
                    @constCast((&[_]metadata_reconciler.MergedGroupStatus{.{
                        .group_id = 10,
                        .leader_known = true,
                        .leader_store_id = 20,
                    }})[0..])
                else
                    @constCast((&[_]metadata_reconciler.MergedGroupStatus{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn createTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, _: tables_api.CreateTableRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.created = true;
        }

        fn waitTableLifecycle(ptr: *anyopaque, table_name: []const u8, expected: TableVisibility) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(TableVisibility.present, expected);
            _ = self.lifecycle_wait_calls.fetchAdd(1, .monotonic);
        }
    };

    const FakeWrites = struct {
        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .create_table = createTable,
                    .local_runtime_statuses = localRuntimeStatuses,
                    .batch = batch,
                },
            };
        }

        fn createTable(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: tables_api.CreateTableRequest) !?void {
            return;
        }

        fn localRuntimeStatuses(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            return error.TestUnexpectedResult;
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }
    };

    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.source());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(alloc, "docs table");
    defer alloc.free(create_body);
    var resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs",
        .content_type = "application/json",
        .body = create_body,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u32, 0), source.lifecycle_wait_calls.load(.monotonic));
}

test "api index status uses read runtime status without consulting write source" {
    const alloc = std.testing.allocator;

    const Response = struct {
        const Stats = struct {
            doc_count: ?u64 = null,
            total_indexed: ?u64 = null,
            node_count: ?u64 = null,
            replay_applied_sequence: ?u64 = null,
            replay_target_sequence: ?u64 = null,
            replay_catch_up_required: ?bool = null,
            catch_up_active: ?bool = null,
            catch_up_applied_sequence: ?u64 = null,
            catch_up_target_sequence: ?u64 = null,
        };

        status: Stats,
        shard_status: ?struct {
            @"10": ?Stats = null,
        } = null,
    };

    const FakeSource = struct {
        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":3}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 10,
                    .table_id = 1,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.TestUnexpectedResult;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.TestUnexpectedResult;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            const indexes = try inner_alloc.alloc(db_mod.types.DBIndexStats, 1);
            errdefer inner_alloc.free(indexes);
            indexes[0] = .{
                .name = try inner_alloc.dupe(u8, "vec"),
                .kind = .dense_vector,
                .doc_count = 9,
                .node_count = 17,
                .replay_applied_sequence = 3,
                .replay_target_sequence = 7,
                .replay_catch_up_required = true,
            };

            const items = try inner_alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
            errdefer {
                db_mod.types.freeDBStats(inner_alloc, .{
                    .doc_count = 9,
                    .index_count = 1,
                    .indexes = indexes,
                });
                inner_alloc.free(items);
            }
            items[0] = .{
                .group_id = 10,
                .stats = .{
                    .doc_count = 9,
                    .index_count = 1,
                    .indexes = indexes,
                },
            };
            return .{ .items = items };
        }
    };

    const FakeWrites = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            const indexes = try inner_alloc.alloc(db_mod.types.DBIndexStats, 1);
            errdefer inner_alloc.free(indexes);
            indexes[0] = .{
                .name = try inner_alloc.dupe(u8, "vec"),
                .kind = .dense_vector,
                .doc_count = 12,
                .node_count = 23,
                .replay_applied_sequence = 5,
                .replay_target_sequence = 11,
                .replay_catch_up_required = true,
            };

            const items = try inner_alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
            errdefer {
                db_mod.types.freeDBStats(inner_alloc, .{
                    .doc_count = 12,
                    .index_count = 1,
                    .indexes = indexes,
                });
                inner_alloc.free(items);
            }
            items[0] = .{
                .group_id = 10,
                .stats = .{
                    .doc_count = 12,
                    .index_count = 1,
                    .indexes = indexes,
                },
            };
            return .{ .items = items };
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), writes.source());

    var resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/vec",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u32, 1), reads.status_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), writes.status_calls.load(.monotonic));
    var parsed = try std.json.parseFromSlice(Response, alloc, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?u64, 9), parsed.value.status.doc_count);
    try std.testing.expectEqual(@as(?u64, 9), parsed.value.status.total_indexed);
    try std.testing.expectEqual(@as(?u64, 17), parsed.value.status.node_count);
    try std.testing.expectEqual(@as(?u64, 7), parsed.value.status.replay_applied_sequence);
    try std.testing.expectEqual(@as(?u64, 7), parsed.value.status.replay_target_sequence);
    try std.testing.expectEqual(@as(?bool, false), parsed.value.status.replay_catch_up_required);
    const shard_status = parsed.value.shard_status.?;
    const shard_10 = shard_status.@"10".?;
    try std.testing.expectEqual(@as(?u64, 9), shard_10.doc_count);
    try std.testing.expectEqual(@as(?u64, 7), shard_10.replay_applied_sequence);
    try std.testing.expectEqual(@as(?u64, 7), shard_10.replay_target_sequence);
    try std.testing.expectEqual(@as(?bool, false), shard_10.replay_catch_up_required);
}

test "api index status falls through to read runtime status when write cache is empty" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":3}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.TestUnexpectedResult;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            return null;
        }
    };

    const FakeWrites = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            return null;
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), writes.source());

    var resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/vec",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u32, 1), reads.status_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), writes.status_calls.load(.monotonic));
}

test "api index status uses propagated remote store runtime status" {
    const alloc = std.testing.allocator;

    const Response = struct {
        const Stats = struct {
            doc_count: ?u64 = null,
            total_indexed: ?u64 = null,
            node_count: ?u64 = null,
            replay_applied_sequence: ?u64 = null,
            replay_target_sequence: ?u64 = null,
            replay_catch_up_required: ?bool = null,
            catch_up_active: ?bool = null,
            catch_up_applied_sequence: ?u64 = null,
            catch_up_target_sequence: ?u64 = null,
        };

        status: Stats,
        shard_status: ?struct {
            @"10": ?Stats = null,
        } = null,
    };

    const FakeSource = struct {
        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":3}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 10,
                    .table_id = 1,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{.{
                    .store_id = 20,
                    .node_id = 30,
                    .runtime_statuses = @constCast((&[_]metadata_table_manager.RuntimeGroupStatusReport{.{
                        .table_id = 1,
                        .table_name = "docs",
                        .group_id = 10,
                        .store_id = 20,
                        .node_id = 30,
                        .updated_at_ns = 99,
                        .freshness = "fresh",
                        .doc_count = 12,
                        .index_count = 1,
                        .async_dense_catch_up_active = true,
                        .indexes = @constCast((&[_]metadata_table_manager.RuntimeIndexStatusReport{.{
                            .name = "vec",
                            .kind = "dense_vector",
                            .doc_count = 12,
                            .node_count = 19,
                            .replay_applied_sequence = 4,
                            .replay_target_sequence = 8,
                            .replay_catch_up_required = true,
                        }})[0..]),
                    }})[0..]),
                }})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.TestUnexpectedResult;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            return null;
        }
    };

    const FakeWrites = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            return null;
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), writes.source());

    var resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/vec",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u32, 1), reads.status_calls.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), writes.status_calls.load(.monotonic));
    var parsed = try std.json.parseFromSlice(Response, alloc, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?u64, 12), parsed.value.status.doc_count);
    try std.testing.expectEqual(@as(?u64, 12), parsed.value.status.total_indexed);
    try std.testing.expectEqual(@as(?u64, 19), parsed.value.status.node_count);
    try std.testing.expectEqual(@as(?u64, 4), parsed.value.status.replay_applied_sequence);
    try std.testing.expectEqual(@as(?u64, 8), parsed.value.status.replay_target_sequence);
    try std.testing.expectEqual(@as(?bool, true), parsed.value.status.replay_catch_up_required);
    try std.testing.expectEqual(@as(?bool, true), parsed.value.status.catch_up_active);
    try std.testing.expectEqual(@as(?u64, 4), parsed.value.status.catch_up_applied_sequence);
    try std.testing.expectEqual(@as(?u64, 8), parsed.value.status.catch_up_target_sequence);
    const shard_status = parsed.value.shard_status.?;
    const shard_10 = shard_status.@"10".?;
    try std.testing.expectEqual(@as(?u64, 12), shard_10.doc_count);
    try std.testing.expectEqual(@as(?bool, true), shard_10.catch_up_active);
}

test "remote runtime status reports replay debt separately from active catch-up" {
    const alloc = std.testing.allocator;

    const report = metadata_table_manager.RuntimeGroupStatusReport{
        .table_id = 1,
        .table_name = "docs",
        .group_id = 10,
        .store_id = 20,
        .node_id = 30,
        .updated_at_ns = 99,
        .freshness = "fresh",
        .doc_count = 56_250,
        .index_count = 1,
        .async_dense_catch_up_active = false,
        .doc_identity = .{
            .namespace_table_id = 1,
            .namespace_shard_id = 10,
            .namespace_range_id = 1001,
            .next_ordinal = 44,
            .rebuild_required = true,
        },
        .doc_set_planning = .{
            .resolved_set_count = 7,
            .ordinal_list_count = 3,
            .missing_ordinal_coverage_count = 1,
            .stale_identity_generation_rejection_count = 2,
        },
        .indexes = @constCast((&[_]metadata_table_manager.RuntimeIndexStatusReport{.{
            .name = "vec",
            .kind = "dense_vector",
            .doc_count = 56_250,
            .node_count = 756,
            .replay_applied_sequence = 225,
            .replay_target_sequence = 300,
            .replay_catch_up_required = true,
        }})[0..]),
    };

    var status = try ApiHttpServer.localRuntimeStatusFromRemoteReport(alloc, report);
    defer status.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), status.stats.indexes.len);
    const index = status.stats.indexes[0];
    try std.testing.expectEqual(@as(u64, 225), index.replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 300), index.replay_target_sequence);
    try std.testing.expectEqual(true, index.replay_catch_up_required);
    try std.testing.expectEqual(false, index.catch_up_active);
    try std.testing.expectEqual(@as(u64, 225), index.catch_up_applied_sequence);
    try std.testing.expectEqual(@as(u64, 300), index.catch_up_target_sequence);
    try std.testing.expectEqual(@as(u64, 1), status.stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(@as(u64, 10), status.stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(@as(u64, 1001), status.stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u32, 44), status.stats.doc_identity.next_ordinal);
    try std.testing.expect(status.stats.doc_identity.rebuild_required);
    try std.testing.expectEqual(@as(u64, 7), status.stats.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 3), status.stats.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 1), status.stats.doc_set_planning.missing_ordinal_coverage_count);
    try std.testing.expectEqual(@as(u64, 2), status.stats.doc_set_planning.stale_identity_generation_rejection_count);
}

test "api index status ignores propagated runtime status from removed owner" {
    const alloc = std.testing.allocator;

    const Response = struct {
        const Stats = struct {
            doc_count: ?u64 = null,
            expected_groups: ?u64 = null,
            reported_groups: ?u64 = null,
            missing_groups: ?u64 = null,
            replay_catch_up_required: ?bool = null,
        };

        status: Stats,
    };

    const FakeSource = struct {
        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":3}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 10,
                    .table_id = 1,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{.{
                    .store_id = 20,
                    .node_id = 30,
                    .runtime_statuses = @constCast((&[_]metadata_table_manager.RuntimeGroupStatusReport{.{
                        .table_id = 1,
                        .table_name = "docs",
                        .group_id = 10,
                        .store_id = 20,
                        .node_id = 30,
                        .updated_at_ns = 99,
                        .freshness = "fresh",
                        .doc_count = 12,
                        .index_count = 1,
                        .indexes = @constCast((&[_]metadata_table_manager.RuntimeIndexStatusReport{.{
                            .name = "vec",
                            .kind = "dense_vector",
                            .doc_count = 12,
                        }})[0..]),
                    }})[0..]),
                }})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{.{
                    .record = .{
                        .group_id = 10,
                        .replica_id = 1,
                        .local_node_id = 31,
                    },
                    .store_id = 21,
                }})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.TestUnexpectedResult;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }

        fn localRuntimeStatuses(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            return null;
        }
    };

    const FakeWrites = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            return null;
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), writes.source());

    var resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/vec",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u32, 0), writes.status_calls.load(.monotonic));
    var parsed = try std.json.parseFromSlice(Response, alloc, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(?u64, null), parsed.value.status.doc_count);
    try std.testing.expectEqual(@as(?u64, null), parsed.value.status.expected_groups);
    try std.testing.expectEqual(@as(?u64, null), parsed.value.status.reported_groups);
    try std.testing.expectEqual(@as(?u64, null), parsed.value.status.missing_groups);
    try std.testing.expectEqual(@as(?bool, null), parsed.value.status.replay_catch_up_required);
}

test "api index status reports missing remote shard as not ready" {
    const alloc = std.testing.allocator;

    const Response = struct {
        const Stats = struct {
            rebuilding: ?bool = null,
            backfill_active: ?bool = null,
            expected_groups: ?u64 = null,
            reported_groups: ?u64 = null,
            missing_groups: ?u64 = null,
            replay_catch_up_required: ?bool = null,
        };

        status: Stats,
        shard_status: ?struct {
            @"10": ?Stats = null,
        } = null,
    };

    const FakeSource = struct {
        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .indexes_json = "{\"vec\":{\"type\":\"embeddings\",\"dimension\":3}}",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 10,
                    .table_id = 1,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
            return try adminSnapshot(ptr);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.TestUnexpectedResult;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.TestUnexpectedResult;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }

        fn localRuntimeStatuses(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            return null;
        }
    };

    const FakeWrites = struct {
        status_calls: std.atomic.Value(u32) = .init(0),

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }

        fn localRuntimeStatuses(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.status_calls.fetchAdd(1, .monotonic);
            return error.TestUnexpectedResult;
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), writes.source());

    var resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/vec",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(u32, 0), writes.status_calls.load(.monotonic));
    var parsed = try std.json.parseFromSlice(Response, alloc, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    if (parsed.value.status.rebuilding) |rebuilding| try std.testing.expect(rebuilding);
    if (parsed.value.status.backfill_active) |active| try std.testing.expect(active);
    if (parsed.value.status.expected_groups) |expected_groups| try std.testing.expect(expected_groups >= 1);
    if (parsed.value.status.reported_groups) |reported_groups| try std.testing.expectEqual(@as(u64, 0), reported_groups);
    if (parsed.value.status.missing_groups) |missing_groups| try std.testing.expect(missing_groups >= 1);
    if (parsed.value.status.replay_catch_up_required) |required| try std.testing.expect(required);
    if (parsed.value.shard_status) |shards| {
        if (shards.@"10") |shard| {
            try std.testing.expectEqual(@as(?bool, true), shard.backfill_active);
            try std.testing.expectEqual(@as(?bool, true), shard.replay_catch_up_required);
        }
    }
}

test "api http server drop table waits for metadata lifecycle absence" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        created: bool = true,
        lifecycle_wait_calls: std.atomic.Value(u32) = .init(0),

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .drop_table = dropTable,
                    .wait_table_lifecycle = waitTableLifecycle,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = if (self.created)
                    @constCast((&[_]metadata_table_manager.TableRecord{.{
                        .table_id = 1,
                        .name = "docs",
                        .placement_role = "data",
                    }})[0..])
                else
                    @constCast((&[_]metadata_table_manager.TableRecord{})[0..]),
                .ranges = if (self.created)
                    @constCast((&[_]metadata_table_manager.RangeRecord{.{
                        .group_id = 10,
                        .table_id = 1,
                        .start_key = "",
                        .end_key = null,
                    }})[0..])
                else
                    @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn dropTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.created = false;
        }

        fn waitTableLifecycle(ptr: *anyopaque, table_name: []const u8, expected: TableVisibility) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(TableVisibility.absent, expected);
            _ = self.lifecycle_wait_calls.fetchAdd(1, .monotonic);
        }
    };

    const FakeWrites = struct {
        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_runtime_statuses = localRuntimeStatuses,
                    .batch = batch,
                },
            };
        }

        fn localRuntimeStatuses(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            return error.TestUnexpectedResult;
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }
    };

    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.source());

    var resp = try server.handle(.{
        .method = .DELETE,
        .uri = "/tables/docs",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 204), resp.status);
    try std.testing.expectEqual(@as(u32, 1), source.lifecycle_wait_calls.load(.monotonic));
}

test "api http server get missing index returns 404 without runtime status lookup" {
    const alloc = std.testing.allocator;

    const FakeSource = struct {
        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}}",
                }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 10,
                    .table_id = 1,
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

    const FakeWrites = struct {
        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_runtime_statuses = localRuntimeStatuses,
                    .batch = batch,
                },
            };
        }

        fn localRuntimeStatuses(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            return error.TestUnexpectedResult;
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return;
        }
    };

    const FakeReads = struct {
        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .preflight_query = preflightQuery,
                    .local_runtime_statuses = localRuntimeStatuses,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.TestUnexpectedResult;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.TestUnexpectedResult;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return null;
        }

        fn preflightQuery(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency, _: u32) !?db_mod.RuntimePreflightSummary {
            return null;
        }

        fn localRuntimeStatuses(_: *anyopaque, _: std.mem.Allocator, _: []const u8) !?runtime_status.LocalTableRuntimeStatuses {
            return error.TestUnexpectedResult;
        }
    };

    var source = FakeSource{};
    var reads = FakeReads{};
    var writes = FakeWrites{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), reads.source(), writes.source());

    var resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/vec",
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), resp.status);
    try std.testing.expectEqualStrings("not found", resp.body);
}

test "api http server serves table metadata routes against real metadata service" {
    const raft_engine = @import("raft_engine");

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

        fn freeDescriptor(ptr: *anyopaque, inner_alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            raft_host.catalog.freeRuntimeBootstrap(inner_alloc, &desc.bootstrap);
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-table-metadata-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-table-metadata-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1988,
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
        .group_id = 1988,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var server = ApiHttpServer.init(std.testing.allocator, .{}, testMetadataServiceSourceWithoutLifecycle(&svc), null, null);

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var create_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs",
        .content_type = "application/json",
        .body = create_body,
    });
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), create_resp.status);
    try std.testing.expectEqualStrings("application/json", create_resp.content_type.?);
    var parsed_create = try std.json.parseFromSlice(metadata_openapi.Table, std.testing.allocator, create_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_create.deinit();
    try std.testing.expectEqualStrings("docs", parsed_create.value.name);
    try std.testing.expectEqualStrings("docs table", parsed_create.value.description.?);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    var detail_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs",
    });
    defer detail_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), detail_resp.status);
    try std.testing.expectEqualStrings("application/json", detail_resp.content_type.?);
    var parsed_detail = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, detail_resp.body, .{});
    defer parsed_detail.deinit();
    try std.testing.expectEqualStrings("docs", parsed_detail.value.name);
    try std.testing.expectEqualStrings("docs table", parsed_detail.value.description.?);
    try std.testing.expect(parsed_detail.value.indexes.map.count() > 0);

    var indexes_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes",
    });
    defer indexes_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), indexes_resp.status);
    try std.testing.expectEqualStrings("application/json", indexes_resp.content_type.?);
    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, indexes_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_indexes.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_indexes.value.array.items.len);
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_indexes.value.array.items[0].object.get("config").?.object.get("name").?.string);

    var index_resp = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs/indexes/full_text_index_v0",
    });
    defer index_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), index_resp.status);
    try std.testing.expectEqualStrings("application/json", index_resp.content_type.?);
    var parsed_index = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, index_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_index.deinit();
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_index.value.object.get("config").?.object.get("name").?.string);

    const update_schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(std.testing.allocator);
    defer std.testing.allocator.free(update_schema_body);
    var update_resp = try server.handle(.{
        .method = .PUT,
        .uri = "/tables/docs/schema",
        .content_type = "application/json",
        .body = update_schema_body,
    });
    defer update_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), update_resp.status);
    try std.testing.expectEqualStrings("application/json", update_resp.content_type.?);
    var parsed_update = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, update_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("docs", parsed_update.value.object.get("name").?.string);
    try std.testing.expect(parsed_update.value.object.get("schema") != null);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    var updated_detail = try server.handle(.{
        .method = .GET,
        .uri = "/tables/docs",
    });
    defer updated_detail.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), updated_detail.status);
    try std.testing.expectEqualStrings("application/json", updated_detail.content_type.?);
    var parsed_updated_detail = try std.json.parseFromSlice(metadata_openapi.TableStatus, std.testing.allocator, updated_detail.body, .{});
    defer parsed_updated_detail.deinit();
    try std.testing.expect(parsed_updated_detail.value.schema != null);
    try std.testing.expect(parsed_updated_detail.value.migration != null);
    try std.testing.expect(parsed_updated_detail.value.migration.?.state.len > 0);

    const valid_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:ok\":{\"title\":\"alpha\",\"status\":\"published\"}}}");
    defer std.testing.allocator.free(valid_batch_body);
    var valid_batch_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/batch",
        .content_type = "application/json",
        .body = valid_batch_body,
    });
    defer valid_batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), valid_batch_resp.status);

    const invalid_batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:bad\":{\"title\":\"alpha\",\"body\":\"unexpected\"}}}");
    defer std.testing.allocator.free(invalid_batch_body);
    var invalid_batch_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/batch",
        .content_type = "application/json",
        .body = invalid_batch_body,
    });
    defer invalid_batch_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), invalid_batch_resp.status);
}

test "api http server create table with replication sources returns encoded table detail" {
    const raft_engine = @import("raft_engine");

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

        fn freeDescriptor(ptr: *anyopaque, inner_alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            raft_host.catalog.freeRuntimeBootstrap(inner_alloc, &desc.bootstrap);
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-table-replication-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-table-replication-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try metadata_service.MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2991,
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
        .group_id = 2991,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var server = ApiHttpServer.init(std.testing.allocator, .{}, testMetadataServiceSourceWithoutLifecycle(&svc), null, null);

    const create_body =
        \\{
        \\  "num_shards": 1,
        \\  "replication_sources": [
        \\    {
        \\      "type": "postgres",
        \\      "dsn": "postgres://localhost:5432/postgres?sslmode=disable",
        \\      "postgres_table": "users",
        \\      "key_template": "id",
        \\      "slot_name": "slot_users",
        \\      "publication_name": "pub_users",
        \\      "on_delete": [{"op": "$delete_document"}]
        \\    }
        \\  ]
        \\}
    ;
    var create_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs",
        .content_type = "application/json",
        .body = create_body,
    });
    defer create_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), create_resp.status);
    try std.testing.expectEqualStrings("application/json", create_resp.content_type.?);
    var parsed_create = try std.json.parseFromSlice(metadata_openapi.Table, std.testing.allocator, create_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_create.deinit();
    try std.testing.expectEqualStrings("docs", parsed_create.value.name);
    try std.testing.expectEqual(@as(usize, 1), parsed_create.value.replication_sources.?.len);
    try std.testing.expectEqualStrings("slot_users", parsed_create.value.replication_sources.?[0].slot_name.?);
    try std.testing.expectEqualStrings("pub_users", parsed_create.value.replication_sources.?[0].publication_name.?);
}

test "api http server lists cluster backups through public route" {
    const alloc = std.testing.allocator;
    const FakeSource = struct {
        fn iface(_: *@This()) StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{ .status = status },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const backup_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/cluster-backups", .{tmp.sub_path});
    defer alloc.free(backup_root);
    const cwd = try std.process.currentPathAlloc(std.testing.io, alloc);
    defer alloc.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(alloc, &.{ cwd, backup_root });
    defer alloc.free(backup_root_abs);
    const location_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{backup_root_abs});
    defer alloc.free(location_uri);

    const table_entries = [_]backups_api.ClusterTableBackupEntry{
        .{
            .name = "docs",
            .table_backup_id = "docs-snap1",
        },
    };
    var manifest = try backups_api.createClusterManifest(alloc, "snap1", location_uri, &table_entries);
    defer manifest.deinit(alloc);
    try backups_api.writeClusterManifest(alloc, backup_root, &manifest);

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), null, null);
    const uri = try std.fmt.allocPrint(alloc, "/backups?location={s}", .{location_uri});
    defer alloc.free(uri);

    var resp = try server.handle(.{
        .method = .GET,
        .uri = uri,
    });
    defer resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("application/json", resp.content_type.?);

    var parsed = try std.json.parseFromSlice(metadata_openapi.BackupListResponse, alloc, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.backups.len);
    try std.testing.expectEqualStrings("snap1", parsed.value.backups[0].backup_id);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.backups[0].tables.len);
    try std.testing.expectEqualStrings("docs", parsed.value.backups[0].tables[0]);
    try std.testing.expectEqualStrings(location_uri, parsed.value.backups[0].location);
    try std.testing.expect(parsed.value.backups[0].timestamp.len > 0);
}

test "api http server backs up and restores a table through public routes" {
    const alloc = std.testing.allocator;
    const StoredTitle = struct {
        title: []const u8,
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/backup-db", .{tmp.sub_path});
    defer alloc.free(path);
    const backup_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/backup-out", .{tmp.sub_path});
    defer alloc.free(backup_root);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 1,
    });

    var read_source = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        created: bool = true,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .create_table = createTable,
                    .drop_table = dropTable,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const tables = if (self.created)
                @constCast((&[_]metadata_table_manager.TableRecord{.{
                    .table_id = 1,
                    .name = "docs",
                    .description = "docs table",
                    .indexes_json = tables_api.default_indexes_json,
                    .placement_role = "data",
                }})[0..])
            else
                @constCast((&[_]metadata_table_manager.TableRecord{})[0..]);
            const ranges = if (self.created)
                @constCast((&[_]metadata_table_manager.RangeRecord{.{
                    .group_id = 1,
                    .table_id = 1,
                    .start_key = "",
                    .end_key = null,
                }})[0..])
            else
                @constCast((&[_]metadata_table_manager.RangeRecord{})[0..]);
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = tables,
                .ranges = ranges,
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn createTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, _: tables_api.CreateTableRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.created = true;
        }

        fn dropTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.created = false;
        }
    };

    var source = FakeSource{};
    var server = ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), write_source.source());

    const cwd = try std.process.currentPathAlloc(std.testing.io, alloc);
    defer alloc.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(alloc, &.{ cwd, backup_root });
    defer alloc.free(backup_root_abs);

    const backup_body = try std.fmt.allocPrint(alloc, "{{\"backup_id\":\"snap1\",\"location\":\"file://{s}\"}}", .{backup_root_abs});
    defer alloc.free(backup_body);
    var backup_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/backup",
        .content_type = "application/json",
        .body = backup_body,
    });
    defer backup_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 201), backup_resp.status);
    try std.testing.expectEqualStrings("application/json", backup_resp.content_type.?);
    var parsed_backup = try std.json.parseFromSlice(std.json.Value, alloc, backup_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_backup.deinit();
    const backup_status = parsed_backup.value.object.get("backup") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("successful", backup_status.string);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
    });
    source.created = false;

    const restore_body = try std.fmt.allocPrint(alloc, "{{\"backup_id\":\"snap1\",\"location\":\"file://{s}\"}}", .{backup_root_abs});
    defer alloc.free(restore_body);
    var restore_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/restore",
        .content_type = "application/json",
        .body = restore_body,
    });
    defer restore_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), restore_resp.status);
    try std.testing.expectEqualStrings("application/json", restore_resp.content_type.?);
    var parsed_restore = try std.json.parseFromSlice(std.json.Value, alloc, restore_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_restore.deinit();
    const restore_status = parsed_restore.value.object.get("restore") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("triggered", restore_status.string);

    var lookup = (try read_source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer lookup.deinit(alloc);
    var parsed_stored = try std.json.parseFromSlice(StoredTitle, alloc, lookup.json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed_stored.deinit();
    const stored = parsed_stored.value;
    try std.testing.expectEqualStrings("alpha", stored.title);
}

test "api http server prefers metadata-owned restore over inline write-source restore" {
    const alloc = std.testing.allocator;

    const RestoreSource = struct {
        restored: bool = false,

        fn iface(self: *@This()) StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .restore_table = restoreTable,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn restoreTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("snap1", backup_id);
            try std.testing.expectEqualStrings("file:///tmp/out", location_uri);
            self.restored = true;
        }
    };

    const FailingWrites = struct {
        fn source() table_writes.TableWriteSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .batch = unsupportedBatch,
                    .create_table = unsupportedCreateTable,
                    .update_schema = unsupportedUpdateSchema,
                    .create_index = unsupportedCreateIndex,
                    .drop_index = unsupportedDropIndex,
                    .backup_table = unsupportedBackupTable,
                    .restore_table = restoreTable,
                    .commit_transaction = unsupportedCommitTransaction,
                    .commit_transaction_with_id = unsupportedCommitTransactionWithId,
                    .batch_group_local = unsupportedBatchGroupLocal,
                    .txn_begin_group_local = unsupportedTxnBeginGroupLocal,
                    .txn_prepare_group_local = unsupportedTxnPrepareGroupLocal,
                    .txn_resolve_group_local = unsupportedTxnResolveGroupLocal,
                },
            };
        }

        fn restoreTable(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: backups_api.TableRestorePlan) anyerror!?void {
            return error.TestUnexpectedResult;
        }

        fn unsupportedBatch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedCreateTable(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: tables_api.CreateTableRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedUpdateSchema(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedCreateIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedDropIndex(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedBackupTable(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: backups_api.TableBackupPlan) anyerror!?[]backups_api.ShardSnapshot {
            return error.UnsupportedOperation;
        }
        fn unsupportedCommitTransaction(_: *anyopaque, _: std.mem.Allocator, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel) anyerror!?distributed_txn.CommitOutcome {
            return error.UnsupportedOperation;
        }
        fn unsupportedCommitTransactionWithId(_: *anyopaque, _: std.mem.Allocator, _: db_mod.types.TxnId, _: u64, _: []const distributed_txn.TableCommitRequest, _: db_mod.types.SyncLevel) anyerror!?distributed_txn.CommitOutcome {
            return error.UnsupportedOperation;
        }
        fn unsupportedBatchGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.BatchRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedTxnBeginGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedTxnPrepareGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }
        fn unsupportedTxnResolveGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) anyerror!?void {
            return error.UnsupportedOperation;
        }
    };

    var restore_source = RestoreSource{};
    var server = ApiHttpServer.init(alloc, .{}, restore_source.iface(), null, FailingWrites.source());
    var restore_resp = try server.handle(.{
        .method = .POST,
        .uri = "/tables/docs/restore",
        .content_type = "application/json",
        .body = "{\"backup_id\":\"snap1\",\"location\":\"file:///tmp/out\"}",
    });
    defer restore_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 202), restore_resp.status);
    try std.testing.expectEqualStrings("application/json", restore_resp.content_type.?);
    var parsed_restore = try std.json.parseFromSlice(std.json.Value, alloc, restore_resp.body, .{
        .allocate = .alloc_always,
    });
    defer parsed_restore.deinit();
    const restore_status = parsed_restore.value.object.get("restore") orelse return error.TestExpectedEqual;
    try std.testing.expectEqualStrings("triggered", restore_status.string);
    try std.testing.expect(restore_source.restored);
}

test "api http server restore metadata spec uses range-scoped restore intent" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const backup_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/restore-spec", .{tmp.sub_path});
    defer alloc.free(backup_root);
    const cwd = try std.process.currentPathAlloc(std.testing.io, alloc);
    defer alloc.free(cwd);
    const backup_root_abs = try std.fs.path.resolve(alloc, &.{ cwd, backup_root });
    defer alloc.free(backup_root_abs);
    const location_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{backup_root_abs});
    defer alloc.free(location_uri);

    const shards = [_]backups_api.ShardSnapshot{
        .{
            .group_id = 7001,
            .start_key = "",
            .end_key = null,
            .snapshot_path = "snap/groups/7001",
        },
    };
    var manifest = try backups_api.createManifest(
        alloc,
        "snap1",
        &.{
            .table_id = 7,
            .name = "docs",
            .description = "docs table",
            .schema_json = "{\"default_type\":\"doc\"}",
            .read_schema_json = "",
            .indexes_json = "{}",
            .replication_sources_json = "[]",
        },
        &shards,
    );
    defer manifest.deinit(alloc);
    try backups_api.writeManifest(alloc, backup_root, &manifest);

    var spec = try loadRestoreMetadataSpec(alloc, "docs", location_uri, "snap1", null);
    defer spec.deinit(alloc);

    try std.testing.expectEqualStrings("", spec.table.restore_backup_id);
    try std.testing.expectEqualStrings("", spec.table.restore_location);
    try std.testing.expectEqual(@as(usize, 1), spec.ranges.len);
    try std.testing.expectEqualStrings("snap1", spec.ranges[0].restore_backup_id);
    try std.testing.expectEqualStrings(location_uri, spec.ranges[0].restore_location);
    try std.testing.expectEqualStrings("snap/groups/7001", spec.ranges[0].restore_snapshot_path);
}

test "api http server join planner uses snapshot stats for low-selectivity lookup joins" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = null },
    };
    var merged = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 101, .doc_count = 100_000, .disk_bytes = 64 * 1024 * 1024, .empty = false },
        .{ .group_id = 201, .doc_count = 100_000, .disk_bytes = 64 * 1024 * 1024, .empty = false },
    };
    var fake = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = merged[0..],
        },
    };

    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:1"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:2"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const plan = try server.planSupportedJoinExecution(std.testing.allocator, "docs", join, parsed_hits.values, .{});
    try std.testing.expectEqual(ApiHttpServer.RightJoinQueryResult.StrategyUsed.index_lookup, plan.strategy);
    try std.testing.expect(plan.used_stats);
    try std.testing.expect(!plan.shuffle_candidate);
    try std.testing.expect(plan.estimated_cost > 0);
}

test "api http server join planner uses foreign source statistics" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            return .{ .rows = &.{}, .total = 0 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 64, .size_bytes = 1024 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    const FakeSource = struct {
        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, DummyForeign.factory);

    var server = ApiHttpServer.init(alloc, .{ .foreign_registry = &registry }, .{
        .ptr = undefined,
        .vtable = &.{
            .status = FakeSource.status,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(alloc,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:1"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:2"}}
        \\]
    );
    defer parsed_hits.deinit(alloc);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("pg_customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var foreign_sources = foreign_mod.PostgresSourceMap{
        .entries = try alloc.alloc(foreign_mod.PostgresNamedConfig, 1),
    };
    defer foreign_sources.deinit(alloc);
    foreign_sources.entries[0] = .{
        .name = try alloc.dupe(u8, "pg_customers"),
        .config = .{
            .dsn = try alloc.dupe(u8, "postgres://db"),
            .postgres_table = try alloc.dupe(u8, "customers"),
            .columns = &.{},
        },
    };

    const plan = try server.planSupportedJoinExecution(alloc, "docs", join, parsed_hits.values, foreign_sources);
    try std.testing.expect(plan.used_stats);
    try std.testing.expectEqual(ApiHttpServer.RightJoinQueryResult.StrategyUsed.broadcast, plan.strategy);
}

test "api http server join planner selects shuffle for large inner joins" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = null },
    };
    var merged = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 101, .doc_count = 200_000, .disk_bytes = 128 * 1024 * 1024, .empty = false },
        .{ .group_id = 201, .doc_count = 100, .disk_bytes = 128 * 1024 * 1024, .empty = false },
    };
    var fake = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = merged[0..],
        },
    };

    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:1"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:2"}},
        \\  {"_id":"doc:3","_source":{"customer_id":"cust:3"}},
        \\  {"_id":"doc:4","_source":{"customer_id":"cust:4"}},
        \\  {"_id":"doc:5","_source":{"customer_id":"cust:5"}},
        \\  {"_id":"doc:6","_source":{"customer_id":"cust:6"}},
        \\  {"_id":"doc:7","_source":{"customer_id":"cust:7"}},
        \\  {"_id":"doc:8","_source":{"customer_id":"cust:8"}},
        \\  {"_id":"doc:9","_source":{"customer_id":"cust:9"}},
        \\  {"_id":"doc:10","_source":{"customer_id":"cust:10"}},
        \\  {"_id":"doc:11","_source":{"customer_id":"cust:11"}},
        \\  {"_id":"doc:12","_source":{"customer_id":"cust:12"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const plan = try server.planSupportedJoinExecution(std.testing.allocator, "docs", join, parsed_hits.values, .{});
    try std.testing.expectEqual(ApiHttpServer.RightJoinQueryResult.StrategyUsed.shuffle, plan.strategy);
    try std.testing.expect(plan.used_stats);
    try std.testing.expect(plan.shuffle_candidate);
    try std.testing.expectEqual(@as(usize, 13), plan.shuffle_partitions);
    try std.testing.expect(!plan.forced_broadcast_fallback);
}

test "api http server join planner falls back from shuffle to broadcast for right joins" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = null },
    };
    var merged = [_]metadata_reconciler.MergedGroupStatus{
        .{ .group_id = 101, .doc_count = 200_000, .disk_bytes = 128 * 1024 * 1024, .empty = false },
        .{ .group_id = 201, .doc_count = 100_000, .disk_bytes = 128 * 1024 * 1024, .empty = false },
    };
    var fake = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = merged[0..],
        },
    };

    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:1"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:2"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .right,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
        .strategy_hint = @constCast("shuffle"),
    };
    const plan = try server.planSupportedJoinExecution(std.testing.allocator, "docs", join, parsed_hits.values, .{});
    try std.testing.expectEqual(ApiHttpServer.RightJoinQueryResult.StrategyUsed.broadcast, plan.strategy);
    try std.testing.expect(plan.shuffle_candidate);
    try std.testing.expect(plan.forced_broadcast_fallback);
}

test "api http server distributed index lookup join uses group-local queries" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        group_201_queries: usize = 0,
        group_202_queries: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .query_group_local = queryGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn queryGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("customers", table_name);
            const wants_a = std.mem.indexOf(u8, req.filter_query_json, "cust:a") != null;
            const wants_z = std.mem.indexOf(u8, req.filter_query_json, "cust:z") != null;
            return switch (group_id) {
                201 => blk: {
                    self.group_201_queries += 1;
                    const body = if (wants_a)
                        "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}}]}}]}"
                    else
                        "{\"responses\":[{\"hits\":{\"total\":0,\"max_score\":0,\"hits\":[]}}]}";
                    break :blk .{ .json = try inner_alloc.dupe(u8, body) };
                },
                202 => blk: {
                    self.group_202_queries += 1;
                    const body = if (wants_z)
                        "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}"
                    else
                        "{\"responses\":[{\"hits\":{\"total\":0,\"max_score\":0,\"hits\":[]}}]}";
                    break :blk .{ .json = try inner_alloc.dupe(u8, body) };
                },
                else => return error.TestUnexpectedResult,
            };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};

    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:z"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var result = try server.executeSupportedRightJoinQuery(std.testing.allocator, fake_reads.source(), join, parsed_hits.values, .{
        .strategy = .index_lookup,
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(ApiHttpServer.RightJoinQueryResult.StrategyUsed.index_lookup, result.strategy_used);
    try std.testing.expect(result.distributed_execution);
    try std.testing.expectEqual(@as(usize, 2), result.groups_queried);
    try std.testing.expectEqual(@as(usize, 1), fake_reads.group_201_queries);
    try std.testing.expectEqual(@as(usize, 1), fake_reads.group_202_queries);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
}

test "api http server distributed shuffle join uses group-local queries" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        group_201_queries: usize = 0,
        group_202_queries: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .query_group_local = queryGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn queryGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("customers", table_name);
            const wants_a = std.mem.indexOf(u8, req.filter_query_json, "cust:a") != null;
            const wants_z = std.mem.indexOf(u8, req.filter_query_json, "cust:z") != null;
            return switch (group_id) {
                201 => blk: {
                    self.group_201_queries += 1;
                    const body = if (wants_a)
                        "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}}]}}]}"
                    else
                        "{\"responses\":[{\"hits\":{\"total\":0,\"max_score\":0,\"hits\":[]}}]}";
                    break :blk .{ .json = try inner_alloc.dupe(u8, body) };
                },
                202 => blk: {
                    self.group_202_queries += 1;
                    const body = if (wants_z)
                        "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}"
                    else
                        "{\"responses\":[{\"hits\":{\"total\":0,\"max_score\":0,\"hits\":[]}}]}";
                    break :blk .{ .json = try inner_alloc.dupe(u8, body) };
                },
                else => return error.TestUnexpectedResult,
            };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};

    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:z"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var result = try server.executeSupportedRightJoinQuery(std.testing.allocator, fake_reads.source(), join, parsed_hits.values, .{
        .strategy = .shuffle,
        .shuffle_partitions = 2,
    });
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(ApiHttpServer.RightJoinQueryResult.StrategyUsed.shuffle, result.strategy_used);
    try std.testing.expect(result.distributed_execution);
    try std.testing.expect(result.groups_queried >= 2);
    try std.testing.expect(fake_reads.group_201_queries > 0);
    try std.testing.expect(fake_reads.group_202_queries > 0);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
}

test "api http server join partition worker joins left rows locally" {
    const FakeReads = struct {
        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8, req: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            try std.testing.expectEqualStrings("customers", table_name);
            const wants_a = std.mem.indexOf(u8, req.filter_query_json, "cust:a") != null;
            const wants_z = std.mem.indexOf(u8, req.filter_query_json, "cust:z") != null;
            const body = if (wants_a and wants_z)
                "{\"responses\":[{\"hits\":{\"total\":2,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}},{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}"
            else if (wants_a)
                "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}}]}}]}"
            else if (wants_z)
                "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}"
            else
                "{\"responses\":[{\"hits\":{\"total\":0,\"max_score\":0,\"hits\":[]}}]}";
            return .{ .json = try inner_alloc.dupe(u8, body) };
        }
    };

    var reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = undefined,
        .vtable = &.{
            .status = unreachableStatus,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a","title":"Alpha"}},
        \\  {"_id":"doc:2","_source":{"customer_id":"cust:z","title":"Zulu"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const body = try server.encodeJoinPartitionRequest(std.testing.allocator, null, join, parsed_hits.values, false, 0, 1, &.{201});
    defer std.testing.allocator.free(body);

    var result = try server.executeJoinPartitionWorkerLocal(std.testing.allocator, reads.source(), 201, "customers", body);
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqual(@as(i64, 2), result.stats.rows_matched);
    const first_name = testOwnedHitSourcePathValue(result.hits[0], "customers.name").?;
    const second_name = testOwnedHitSourcePathValue(result.hits[1], "customers.name").?;
    try std.testing.expectEqualStrings("Alice", first_name.string);
    try std.testing.expectEqualStrings("Zoe", second_name.string);
}

test "api http server join partition worker tracks matched right ids for right joins" {
    const FakeReads = struct {
        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            try std.testing.expectEqualStrings("customers", table_name);
            return .{ .json = try inner_alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":2,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}},{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}") };
        }
    };

    var reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = undefined,
        .vtable = &.{
            .status = unreachableStatus,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a","title":"Alpha"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .right,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const body = try server.encodeJoinPartitionRequest(std.testing.allocator, null, join, parsed_hits.values, false, 0, 1, &.{201});
    defer std.testing.allocator.free(body);

    var result = try server.executeJoinPartitionWorkerLocal(std.testing.allocator, reads.source(), 201, "customers", body);
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(i64, 1), result.stats.rows_matched);
    try std.testing.expectEqual(@as(usize, 1), result.matched_right_ids.len);
    try std.testing.expectEqualStrings("cust:a", result.matched_right_ids[0]);
    const right_name = testOwnedHitSourcePathValue(result.hits[0], "customers.name").?;
    try std.testing.expectEqualStrings("Alice", right_name.string);
}

test "api http server distributed shuffle join dispatches worker partitions" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        worker_calls: usize = 0,
        group_local_queries: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                    .query_group_local = queryGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn queryGroupLocal(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.group_local_queries += 1;
            return error.TestUnexpectedResult;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.worker_calls += 1;
            try std.testing.expectEqualStrings("customers", table_name);
            var parsed = try std.json.parseFromSlice(distributed_join.EncodedJoinPartitionRequest, inner_alloc, body, .{});
            defer parsed.deinit();
            const left_hits = parsed.value.left_hits;

            var hits = std.json.Array.init(inner_alloc);
            defer {
                for (hits.items) |*item| ApiHttpServer.deinitJsonValue(inner_alloc, item);
                hits.deinit();
            }
            for (left_hits) |hit| {
                var joined = try ApiHttpServer.cloneJsonValue(inner_alloc, hit);
                const src = joined.object.getPtr("_source").?;
                const customer_id = ApiHttpServer.extractJoinValueFromHit(hit, "customer_id").?.string;
                const customer_name = if (std.mem.eql(u8, customer_id, "cust:a")) "Alice" else "Zoe";
                try src.object.put(inner_alloc, try std.fmt.allocPrint(inner_alloc, "{s}.{s}", .{ table_name, "name" }), .{ .string = try inner_alloc.dupe(u8, customer_name) });
                try src.object.put(inner_alloc, try std.fmt.allocPrint(inner_alloc, "{s}.{s}", .{ table_name, "worker_group" }), .{ .integer = @intCast(group_id) });
                try hits.append(joined);
            }

            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", .{ .array = hits });
            hits = std.json.Array.init(inner_alloc);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = @intCast(left_hits.len) });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = @intCast(left_hits.len) });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = @intCast(left_hits.len) });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    const partition_zero_key = if (ApiHttpServer.partitionForJoinValue(.{ .string = "cust:a" }, 2) == 0) "cust:a" else "cust:z";
    const partition_one_key = if (std.mem.eql(u8, partition_zero_key, "cust:a")) "cust:z" else "cust:a";
    const hits_body = try std.fmt.allocPrint(std.testing.allocator,
        \\[
        \\  {{"_id":"doc:1","_source":{{"customer_id":"{s}"}}}},
        \\  {{"_id":"doc:2","_source":{{"customer_id":"{s}"}}}}
        \\]
    , .{ partition_zero_key, partition_one_key });
    defer std.testing.allocator.free(hits_body);
    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator, hits_body);
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var result = (try server.executeSupportedDistributedJoinPartitions(std.testing.allocator, fake_reads.source(), null, join, parsed_hits.values, false, .{
        .strategy = .shuffle,
        .shuffle_partitions = 2,
    }, null)).?;
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqual(@as(usize, 2), result.groups_queried);
    try std.testing.expectEqual(@as(usize, 2), fake_reads.worker_calls);
    try std.testing.expectEqual(@as(usize, 0), fake_reads.group_local_queries);
}

test "api http server distributed right shuffle appends unmatched right rows after worker phase" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        worker_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .query_group_local = queryGroupLocal,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, table_name: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            try std.testing.expectEqualStrings("customers", table_name);
            return .{ .json = try inner_alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":2,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}},{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}") };
        }

        fn queryGroupLocal(_: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            try std.testing.expectEqualStrings("customers", table_name);
            return switch (group_id) {
                201 => .{ .json = try inner_alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}}]}}]}") },
                202 => .{ .json = try inner_alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}") },
                else => return error.TestUnexpectedResult,
            };
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.worker_calls += 1;

            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            if (group_id == 201) {
                try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"title\":\"Alpha\",\"customers.name\":\"Alice\"}}"));
            }
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = @intFromBool(group_id == 201) });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = @intFromBool(group_id == 201) });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = @intFromBool(group_id == 201) });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            var matched_right_ids = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &matched_right_ids);
            if (group_id == 201) {
                try matched_right_ids.array.append(.{ .string = try inner_alloc.dupe(u8, "cust:a") });
            }
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "matched_right_ids", matched_right_ids);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a","title":"Alpha"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);
    var left_fields = try parseTestStringValuesAlloc(std.testing.allocator,
        \\["title"]
    );
    defer left_fields.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .right,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 701, null, join, parsed_hits.values, left_fields.values, false, 1);
    defer std.testing.allocator.free(body);
    var result = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqual(@as(i64, 1), result.stats.rows_unmatched_right);
    const unmatched = result.hits[1];
    try std.testing.expect(testOwnedHitSourcePathValue(unmatched, "title").? == .null);
    const unmatched_customer_name = testOwnedHitSourcePathValue(unmatched, "customers.name").?;
    try std.testing.expectEqualStrings("Zoe", unmatched_customer_name.string);
}

test "api http server join partition worker fetches remote partition rows" {
    const FakeReads = struct {
        remote_key: []const u8,
        local_group_queries: usize = 0,
        remote_join_rows_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .query_group_local = queryGroupLocal,
                    .join_rows_group_local = joinRowsGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn queryGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("customers", table_name);
            return switch (group_id) {
                201 => blk: {
                    self.local_group_queries += 1;
                    break :blk .{ .json = try inner_alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":1,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}}]}}]}") };
                },
                else => return error.TestUnexpectedResult,
            };
        }

        fn joinRowsGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("customers", table_name);
            try std.testing.expectEqual(@as(u64, 202), group_id);
            self.remote_join_rows_calls += 1;

            var parsed = try std.json.parseFromSlice(distributed_join.EncodedJoinRowsRequest, inner_alloc, body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(u64, 2), parsed.value.partition_count.?);

            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            const body_json = try std.fmt.allocPrint(inner_alloc, "{{\"_id\":\"{s}\",\"_score\":0,\"_source\":{{\"name\":\"Zoe\"}}}}", .{self.remote_key});
            defer inner_alloc.free(body_json);
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, body_json));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = undefined,
        .vtable = &.{
            .status = unreachableStatus,
        },
    }, null, null);
    defer server.deinit();

    const remote_key = blk: {
        const wanted = ApiHttpServer.partitionForJoinValue(.{ .string = "cust:a" }, 2);
        const candidates = [_][]const u8{ "cust:b", "cust:c", "cust:d", "cust:e", "cust:f", "cust:g", "cust:h", "cust:i", "cust:j", "cust:k", "cust:l", "cust:m", "cust:n", "cust:o", "cust:p", "cust:q", "cust:r", "cust:s", "cust:t", "cust:u", "cust:v", "cust:w", "cust:x", "cust:y", "cust:z" };
        for (candidates) |candidate| {
            if (ApiHttpServer.partitionForJoinValue(.{ .string = candidate }, 2) == wanted) break :blk candidate;
        }
        return error.TestUnexpectedResult;
    };
    const partition_index = ApiHttpServer.partitionForJoinValue(.{ .string = "cust:a" }, 2);
    var reads = FakeReads{ .remote_key = remote_key };

    const hits_body = try std.fmt.allocPrint(std.testing.allocator,
        \\[
        \\  {{"_id":"doc:1","_source":{{"customer_id":"cust:a"}}}},
        \\  {{"_id":"doc:2","_source":{{"customer_id":"{s}"}}}}
        \\]
    , .{remote_key});
    defer std.testing.allocator.free(hits_body);
    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator, hits_body);
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const body = try server.encodeJoinPartitionRequest(std.testing.allocator, null, join, parsed_hits.values, false, partition_index, 2, &.{ 201, 202 });
    defer std.testing.allocator.free(body);

    var result = try server.executeJoinPartitionWorkerLocal(std.testing.allocator, reads.source(), 201, "customers", body);
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(i64, 2), result.stats.rows_matched);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqual(@as(usize, 1), reads.local_group_queries);
    try std.testing.expectEqual(@as(usize, 1), reads.remote_join_rows_calls);
}

test "api http server distributed shuffle can use remote finalizer worker" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        finalizer_calls: usize = 0,
        partition_calls: usize = 0,
        last_finalizer_group_id: ?u64 = null,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_finalize_group_local = joinFinalizeGroupLocal,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.partition_calls += 1;
            return error.TestUnexpectedResult;
        }

        fn joinFinalizeGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.finalizer_calls += 1;
            try std.testing.expect(group_id == 201 or group_id == 202);
            self.last_finalizer_group_id = group_id;
            try std.testing.expectEqualStrings("customers", table_name);
            var parsed = try std.json.parseFromSlice(distributed_join.EncodedJoinFinalizeRequest, inner_alloc, body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(u64, 2), parsed.value.shuffle_partitions.?);

            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var result = (try server.executeSupportedDistributedJoinFinalized(std.testing.allocator, fake_reads.source(), join, parsed_hits.values, &.{}, false, .{
        .strategy = .shuffle,
        .shuffle_partitions = 2,
    })).?;
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.finalizer_calls);
    try std.testing.expectEqual(@as(usize, 0), fake_reads.partition_calls);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(ApiHttpServer.JoinShuffleExecutionMode.transient, result.execution_mode);
    try std.testing.expectEqual(@as(?u64, null), result.job_id);
    try std.testing.expectEqual(fake_reads.last_finalizer_group_id, result.finalizer_group_id);
    try std.testing.expectEqual(@as(usize, 0), result.finalizer_retries);
    try std.testing.expect(!result.coordinator_finalized);
    try std.testing.expectEqual(@as(usize, 1), result.finalizer_attempts.len);
    try std.testing.expectEqual(fake_reads.last_finalizer_group_id.?, result.finalizer_attempts[0].worker_group_id);
    try std.testing.expect(result.finalizer_attempts[0].succeeded);
}

test "api http server distributed shuffle prefers shared metadata lease owner" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const join_store_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-join-lease-store.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(join_store_path);

    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,
        lease: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,
        last_upsert: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn getJoinShuffleLease(ptr: *anyopaque, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const lease = self.lease orelse return null;
            if (lease.job_id != job_id) return null;
            return lease;
        }

        fn upsertJoinShuffleLease(ptr: *anyopaque, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_upsert = record;
            self.lease = record;
        }
    };

    const FakeReads = struct {
        finalizer_calls: usize = 0,
        last_finalizer_group_id: ?u64 = null,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_finalize_group_local = joinFinalizeGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinFinalizeGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.finalizer_calls += 1;
            self.last_finalizer_group_id = group_id;
            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{
        .join_job_store_path = join_store_path,
    }, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            .get_join_shuffle_lease = FakeSource.getJoinShuffleLease,
            .upsert_join_shuffle_lease = FakeSource.upsertJoinShuffleLease,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const job_id = try server.stableDistributedJoinJobId(std.testing.allocator, join, parsed_hits.values, false, 3);
    fake_source.lease = .{
        .job_id = job_id,
        .owner_group_id = 202,
        .expires_at_ms = std.math.maxInt(u64),
    };

    var result = (try server.executeSupportedDistributedJoinFinalized(std.testing.allocator, fake_reads.source(), join, parsed_hits.values, &.{}, false, .{
        .strategy = .shuffle,
        .shuffle_partitions = 3,
    })).?;
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.finalizer_calls);
    try std.testing.expectEqual(ApiHttpServer.JoinShuffleExecutionMode.durable, result.execution_mode);
    try std.testing.expectEqual(@as(?u64, 202), fake_reads.last_finalizer_group_id);
    try std.testing.expectEqual(@as(?u64, 202), result.finalizer_group_id);
    try std.testing.expect(result.job_id != null);
    try std.testing.expect(fake_source.last_upsert != null);
    try std.testing.expectEqual(@as(u64, job_id), fake_source.last_upsert.?.job_id);
    try std.testing.expectEqual(@as(u64, 202), fake_source.last_upsert.?.owner_group_id);
}

test "api http server distributed shuffle updates shared metadata lease after owner handoff" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const join_store_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-join-lease-handoff-store.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(join_store_path);

    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,
        lease: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,
        last_upsert: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn getJoinShuffleLease(ptr: *anyopaque, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const lease = self.lease orelse return null;
            if (lease.job_id != job_id) return null;
            return lease;
        }

        fn upsertJoinShuffleLease(ptr: *anyopaque, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_upsert = record;
            self.lease = record;
        }
    };

    const FakeReads = struct {
        attempts_201: usize = 0,
        attempts_202: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_finalize_group_local = joinFinalizeGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinFinalizeGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return switch (group_id) {
                201 => blk: {
                    self.attempts_201 += 1;
                    break :blk null;
                },
                202 => blk: {
                    self.attempts_202 += 1;
                    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
                    defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
                    var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
                    errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
                    try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
                    var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
                    errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
                    try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
                    break :blk .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
                },
                else => return error.TestUnexpectedResult,
            };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{
        .join_job_store_path = join_store_path,
    }, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            .get_join_shuffle_lease = FakeSource.getJoinShuffleLease,
            .upsert_join_shuffle_lease = FakeSource.upsertJoinShuffleLease,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const job_id = try server.stableDistributedJoinJobId(std.testing.allocator, join, parsed_hits.values, false, 3);
    fake_source.lease = .{
        .job_id = job_id,
        .owner_group_id = 201,
        .expires_at_ms = std.math.maxInt(u64),
    };

    var result = (try server.executeSupportedDistributedJoinFinalized(std.testing.allocator, fake_reads.source(), join, parsed_hits.values, &.{}, false, .{
        .strategy = .shuffle,
        .shuffle_partitions = 3,
    })).?;
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.attempts_201);
    try std.testing.expectEqual(@as(usize, 1), fake_reads.attempts_202);
    try std.testing.expectEqual(@as(?u64, 202), result.finalizer_group_id);
    try std.testing.expectEqual(@as(usize, 1), result.finalizer_retries);
    try std.testing.expect(fake_source.last_upsert != null);
    try std.testing.expectEqual(@as(u64, job_id), fake_source.last_upsert.?.job_id);
    try std.testing.expectEqual(@as(u64, 202), fake_source.last_upsert.?.owner_group_id);
}

test "api http server distributed shuffle retries alternate finalizer worker" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        attempts_201: usize = 0,
        attempts_202: usize = 0,
        call_order: [2]u64 = .{ 0, 0 },
        call_count: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_finalize_group_local = joinFinalizeGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinFinalizeGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_order[self.call_count] = group_id;
            self.call_count += 1;
            return switch (group_id) {
                201 => blk: {
                    self.attempts_201 += 1;
                    if (self.call_count == 1) break :blk null;
                    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
                    defer ApiHttpServer.deinitJsonValue(alloc, &root);
                    var hits = std.json.Value{ .array = std.json.Array.init(alloc) };
                    errdefer ApiHttpServer.deinitJsonValue(alloc, &hits);
                    try hits.array.append(try parseOwnedJsonValueAlloc(alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "hits", hits);
                    var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
                    errdefer ApiHttpServer.deinitJsonValue(alloc, &stats);
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_matched", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "stats", stats);
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "worker_retries", .{ .integer = 0 });
                    break :blk .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(alloc, root) };
                },
                202 => blk: {
                    self.attempts_202 += 1;
                    if (self.call_count == 1) break :blk null;
                    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
                    defer ApiHttpServer.deinitJsonValue(alloc, &root);
                    var hits = std.json.Value{ .array = std.json.Array.init(alloc) };
                    errdefer ApiHttpServer.deinitJsonValue(alloc, &hits);
                    try hits.array.append(try parseOwnedJsonValueAlloc(alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "hits", hits);
                    var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
                    errdefer ApiHttpServer.deinitJsonValue(alloc, &stats);
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_matched", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "stats", stats);
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "worker_retries", .{ .integer = 0 });
                    break :blk .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(alloc, root) };
                },
                else => return error.TestUnexpectedResult,
            };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var result = (try server.executeSupportedDistributedJoinFinalized(std.testing.allocator, fake_reads.source(), join, parsed_hits.values, &.{}, false, .{
        .strategy = .shuffle,
        .shuffle_partitions = 2,
    })).?;
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.attempts_201);
    try std.testing.expectEqual(@as(usize, 1), fake_reads.attempts_202);
    try std.testing.expectEqual(ApiHttpServer.JoinShuffleExecutionMode.transient, result.execution_mode);
    try std.testing.expectEqual(@as(?u64, null), result.job_id);
    try std.testing.expectEqual(@as(?u64, fake_reads.call_order[1]), result.finalizer_group_id);
    try std.testing.expectEqual(@as(usize, 1), result.finalizer_retries);
    try std.testing.expect(!result.coordinator_finalized);
    try std.testing.expectEqual(@as(usize, 2), result.finalizer_attempts.len);
    try std.testing.expectEqual(fake_reads.call_order[0], result.finalizer_attempts[0].worker_group_id);
    try std.testing.expect(!result.finalizer_attempts[0].succeeded);
    try std.testing.expectEqual(fake_reads.call_order[1], result.finalizer_attempts[1].worker_group_id);
    try std.testing.expect(result.finalizer_attempts[1].succeeded);
}

test "api http server distributed shuffle finalizer can resume completed job by id" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        partition_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.partition_calls += 1;
            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 77, null, join, parsed_hits.values, &.{}, false, 1);
    defer std.testing.allocator.free(body);

    var first = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
    defer first.deinit(std.testing.allocator);
    var second = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
    defer second.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.partition_calls);
    try std.testing.expectEqual(@as(?u64, 77), second.job_id);
    try std.testing.expectEqual(ApiHttpServer.JoinShuffleJobPhase.succeeded, second.job_phase.?);
    try std.testing.expectEqual(@as(usize, 1), second.completed_partitions);
}

test "api http server distributed shuffle finalizer resumes persisted job after restart" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        partition_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.partition_calls += 1;
            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const session_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-join-job-sessions", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path);

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{ .session_store_path = session_path }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            },
        }, null, null);
        defer server.deinit();

        const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 88, null, join, parsed_hits.values, &.{}, false, 1);
        defer std.testing.allocator.free(body);
        var first = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
        defer first.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 1), fake_reads.partition_calls);
    }

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{ .session_store_path = session_path }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            },
        }, null, null);
        defer server.deinit();

        const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 88, null, join, parsed_hits.values, &.{}, false, 1);
        defer std.testing.allocator.free(body);
        var second = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
        defer second.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 1), fake_reads.partition_calls);
        try std.testing.expectEqual(@as(?u64, 88), second.job_id);
        try std.testing.expectEqual(ApiHttpServer.JoinShuffleJobPhase.succeeded, second.job_phase.?);
    }
}

test "api http server distributed shuffle expired persisted job is recomputed after restart" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        partition_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.partition_calls += 1;
            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(alloc) };
            errdefer ApiHttpServer.deinitJsonValue(alloc, &hits);
            try hits.array.append(try parseOwnedJsonValueAlloc(alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
            try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(alloc, root) };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const session_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-join-job-expire", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path);

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{
            .session_store_path = session_path,
            .join_job_retention_ms = 0,
        }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            },
        }, null, null);
        defer server.deinit();

        const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 99, null, join, parsed_hits.values, &.{}, false, 1);
        defer std.testing.allocator.free(body);
        var first = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
        defer first.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 1), fake_reads.partition_calls);
    }

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{
            .session_store_path = session_path,
            .join_job_retention_ms = 0,
        }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            },
        }, null, null);
        defer server.deinit();

        const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 99, null, join, parsed_hits.values, &.{}, false, 1);
        defer std.testing.allocator.free(body);
        var second = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
        defer second.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 2), fake_reads.partition_calls);
        try std.testing.expectEqual(ApiHttpServer.JoinShuffleJobPhase.succeeded, second.job_phase.?);
    }
}

test "api http server distributed shuffle resumes persisted partial job after restart" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        partition_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: u64, _: []const u8, body: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var parsed = try std.json.parseFromSlice(distributed_join.EncodedJoinPartitionRequest, inner_alloc, body, .{});
            defer parsed.deinit();
            const partition_index: usize = @intCast(parsed.value.partition_index orelse return error.InvalidQueryRequest);
            self.partition_calls += 1;

            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            const hit_json = switch (partition_index) {
                1 => "{\"_id\":\"doc:2\",\"_source\":{\"customer_id\":\"cust:z\",\"customers.name\":\"Bob\"}}",
                else => return error.TestUnexpectedResult,
            };
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, hit_json));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const session_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-join-job-partial-resume", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path);

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};

    const partition_zero_key = if (ApiHttpServer.partitionForJoinValue(.{ .string = "cust:a" }, 2) == 0) "cust:a" else "cust:z";
    const partition_one_key = if (std.mem.eql(u8, partition_zero_key, "cust:a")) "cust:z" else "cust:a";
    const hits_body = try std.fmt.allocPrint(std.testing.allocator,
        \\[
        \\  {{"_id":"doc:1","_source":{{"customer_id":"{s}"}}}},
        \\  {{"_id":"doc:2","_source":{{"customer_id":"{s}"}}}}
        \\]
    , .{ partition_zero_key, partition_one_key });
    defer std.testing.allocator.free(hits_body);
    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator, hits_body);
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{ .session_store_path = session_path }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            },
        }, null, null);
        defer server.deinit();

        try server.recordJoinJobStart(5150, 201, 2);
        const owned_hits = try std.testing.allocator.alloc(std.json.Value, 1);
        errdefer std.testing.allocator.free(owned_hits);
        owned_hits[0] = try parseOwnedJsonValueAlloc(std.testing.allocator, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}");
        var partial_result: ApiHttpServer.JoinPartitionExecutionResult = .{
            .hits = owned_hits,
            .stats = .{
                .left_rows_scanned = 1,
                .right_rows_scanned = 1,
                .rows_matched = 1,
                .rows_unmatched_left = 0,
                .rows_unmatched_right = 0,
            },
            .job_id = 5150,
            .job_phase = .finalizing,
            .total_partitions = 2,
            .completed_partitions = 1,
            .worker_retries = 0,
            .worker_attempts = try std.testing.allocator.dupe(ApiHttpServer.JoinPartitionExecutionResult.WorkerAttempt, &.{
                .{ .partition_index = 0, .worker_group_id = 201, .succeeded = true },
            }),
        };
        defer partial_result.deinit(std.testing.allocator);
        try server.recordJoinJobProgress(5150, 1, partial_result);
    }

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{ .session_store_path = session_path }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            },
        }, null, null);
        defer server.deinit();

        const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 5150, null, join, parsed_hits.values, &.{}, false, 2);
        defer std.testing.allocator.free(body);
        var resumed = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
        defer resumed.deinit(std.testing.allocator);

        try std.testing.expectEqual(@as(usize, 1), fake_reads.partition_calls);
        try std.testing.expectEqual(ApiHttpServer.JoinShuffleJobPhase.succeeded, resumed.job_phase.?);
        try std.testing.expectEqual(@as(usize, 2), resumed.hits.len);
        try std.testing.expectEqual(@as(usize, 2), resumed.completed_partitions);
    }
}

test "api http server distributed shuffle imports prior owner partial state during handoff" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        server: *ApiHttpServer,
        snapshot_calls: usize = 0,
        partition_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                    .join_job_state_group_local = joinJobStateGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinJobStateGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id != 201) return null;
            self.snapshot_calls += 1;

            const owned_hits = try inner_alloc.alloc(std.json.Value, 1);
            errdefer inner_alloc.free(owned_hits);
            owned_hits[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}");
            var partial_result: ApiHttpServer.JoinPartitionExecutionResult = .{
                .hits = owned_hits,
                .stats = .{
                    .left_rows_scanned = 1,
                    .right_rows_scanned = 1,
                    .rows_matched = 1,
                    .rows_unmatched_left = 0,
                    .rows_unmatched_right = 0,
                },
                .job_id = 900,
                .job_phase = .finalizing,
                .total_partitions = 2,
                .completed_partitions = 1,
                .worker_retries = 0,
                .worker_attempts = try inner_alloc.dupe(ApiHttpServer.JoinPartitionExecutionResult.WorkerAttempt, &.{
                    .{ .partition_index = 0, .worker_group_id = 201, .succeeded = true },
                }),
            };
            defer partial_result.deinit(inner_alloc);
            const partial_response = try self.server.encodeJoinPartitionResponse(inner_alloc, partial_result);
            defer inner_alloc.free(partial_response);

            var state: ApiHttpServer.JoinShuffleJobState = .{
                .owner_group_id = 201,
                .phase = .finalizing,
                .total_partitions = 2,
                .completed_partitions = 1,
                .next_partition_index = 1,
                .worker_retries = 0,
                .partial_response = try inner_alloc.dupe(u8, partial_response),
                .expires_at_millis = std.math.maxInt(u64),
            };
            defer state.deinit(inner_alloc);
            return .{ .json = try self.server.encodeJoinJobState(inner_alloc, 900, state) };
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: u64, _: []const u8, body: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.partition_calls += 1;
            var parsed = try std.json.parseFromSlice(distributed_join.EncodedJoinPartitionRequest, inner_alloc, body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(u64, 1), parsed.value.partition_index.?);

            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:2\",\"_source\":{\"customer_id\":\"cust:z\",\"customers.name\":\"Bob\"}}"));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();
    var fake_reads = FakeReads{ .server = &server };

    const partition_zero_key = if (ApiHttpServer.partitionForJoinValue(.{ .string = "cust:a" }, 2) == 0) "cust:a" else "cust:z";
    const partition_one_key = if (std.mem.eql(u8, partition_zero_key, "cust:a")) "cust:z" else "cust:a";
    const hits_body = try std.fmt.allocPrint(std.testing.allocator,
        \\[
        \\  {{"_id":"doc:1","_source":{{"customer_id":"{s}"}}}},
        \\  {{"_id":"doc:2","_source":{{"customer_id":"{s}"}}}}
        \\]
    , .{ partition_zero_key, partition_one_key });
    defer std.testing.allocator.free(hits_body);
    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator, hits_body);
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 900, 201, join, parsed_hits.values, &.{}, false, 2);
    defer std.testing.allocator.free(body);
    var resumed = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 202, "customers", body);
    defer resumed.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.snapshot_calls);
    try std.testing.expectEqual(@as(usize, 1), fake_reads.partition_calls);
    try std.testing.expectEqual(ApiHttpServer.JoinShuffleJobPhase.succeeded, resumed.job_phase.?);
    try std.testing.expectEqual(@as(usize, 2), resumed.hits.len);
    try std.testing.expectEqual(@as(?u64, 201), resumed.imported_owner_group_id);
    try std.testing.expect(resumed.imported_partial_state);
    try std.testing.expect(!resumed.imported_cached_result);
}

test "api http server distributed shuffle reuses completed result from prior owner" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const join_store_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-join-owner-result-reuse-store.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(join_store_path);

    const FakeStatusSource = struct {
        snapshot: metadata_api.AdminSnapshot,
        lease: metadata_table_manager.ShuffleJoinLeaseRecord,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn getJoinShuffleLease(ptr: *anyopaque, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.lease.job_id != job_id) return null;
            return self.lease;
        }

        fn upsertJoinShuffleLease(_: *anyopaque, _: metadata_table_manager.ShuffleJoinLeaseRecord) !void {}
    };

    const FakeReads = struct {
        server: *ApiHttpServer,
        job_id: u64,
        snapshot_calls: usize = 0,
        finalize_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_job_state_group_local = joinJobStateGroupLocal,
                    .join_finalize_group_local = joinFinalizeGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinJobStateGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id != 201) return null;
            self.snapshot_calls += 1;

            const owned_hits = try inner_alloc.alloc(std.json.Value, 1);
            errdefer inner_alloc.free(owned_hits);
            owned_hits[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}");
            var completed: ApiHttpServer.JoinPartitionExecutionResult = .{
                .hits = owned_hits,
                .stats = .{
                    .left_rows_scanned = 1,
                    .right_rows_scanned = 1,
                    .rows_matched = 1,
                    .rows_unmatched_left = 0,
                    .rows_unmatched_right = 0,
                },
                .job_id = self.job_id,
                .job_phase = .succeeded,
                .total_partitions = 1,
                .completed_partitions = 1,
            };
            defer completed.deinit(inner_alloc);
            const cached_response = try self.server.encodeJoinPartitionResponse(inner_alloc, completed);
            defer inner_alloc.free(cached_response);

            var state: ApiHttpServer.JoinShuffleJobState = .{
                .owner_group_id = 201,
                .phase = .succeeded,
                .total_partitions = 1,
                .completed_partitions = 1,
                .next_partition_index = 1,
                .cached_response = try inner_alloc.dupe(u8, cached_response),
                .expires_at_millis = std.math.maxInt(u64),
            };
            defer state.deinit(inner_alloc);
            return .{ .json = try self.server.encodeJoinJobState(inner_alloc, self.job_id, state) };
        }

        fn joinFinalizeGroupLocal(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.finalize_calls += 1;
            return null;
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_status = FakeStatusSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
        .lease = .{
            .job_id = 0,
            .owner_group_id = 201,
            .expires_at_ms = std.math.maxInt(u64),
        },
    };
    var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{
        .join_job_store_path = join_store_path,
    }, .{
        .ptr = &fake_status,
        .vtable = &.{
            .status = FakeStatusSource.status,
            .admin_snapshot = FakeStatusSource.adminSnapshot,
            .free_admin_snapshot = FakeStatusSource.freeAdminSnapshot,
            .get_join_shuffle_lease = FakeStatusSource.getJoinShuffleLease,
            .upsert_join_shuffle_lease = FakeStatusSource.upsertJoinShuffleLease,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const job_id = try server.stableDistributedJoinJobId(std.testing.allocator, join, parsed_hits.values, false, 3);
    fake_status.lease.job_id = job_id;
    var fake_reads = FakeReads{ .server = &server, .job_id = job_id };

    var result = (try server.executeSupportedDistributedJoinFinalized(std.testing.allocator, fake_reads.source(), join, parsed_hits.values, &.{}, false, .{
        .strategy = .shuffle,
        .shuffle_partitions = 3,
    })).?;
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.snapshot_calls);
    try std.testing.expectEqual(@as(usize, 0), fake_reads.finalize_calls);
    try std.testing.expectEqual(ApiHttpServer.JoinShuffleJobPhase.succeeded, result.job_phase.?);
    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqual(@as(?u64, 201), result.imported_owner_group_id);
    try std.testing.expect(!result.imported_partial_state);
    try std.testing.expect(result.imported_cached_result);
}

test "api http server distributed shuffle failure clears shared metadata lease" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,
        removed_job_id: ?u64 = null,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn removeJoinShuffleLease(ptr: *anyopaque, job_id: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.removed_job_id = job_id;
        }
    };

    const FakeReads = struct {
        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            return error.TestExpectedError;
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
            .remove_join_shuffle_lease = FakeSource.removeJoinShuffleLease,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 707, null, join, parsed_hits.values, &.{}, false, 1);
    defer std.testing.allocator.free(body);

    try std.testing.expectError(error.TestExpectedError, server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body));
    try std.testing.expectEqual(@as(?u64, 707), fake_source.removed_job_id);
}

test "api http server distributed shuffle expired persisted job clears shared metadata lease after restart" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,
        lease: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,
        removed_job_id: ?u64 = null,
        upsert_count: usize = 0,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn upsertJoinShuffleLease(ptr: *anyopaque, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.upsert_count += 1;
            self.lease = record;
        }

        fn removeJoinShuffleLease(ptr: *anyopaque, job_id: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.removed_job_id = job_id;
            if (self.lease) |lease| {
                if (lease.job_id == job_id) self.lease = null;
            }
        }
    };

    const FakeReads = struct {
        partition_calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.partition_calls += 1;
            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer ApiHttpServer.deinitJsonValue(inner_alloc, &root);
            var hits = std.json.Value{ .array = std.json.Array.init(inner_alloc) };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &hits);
            try hits.array.append(try parseOwnedJsonValueAlloc(inner_alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "hits", hits);
            var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
            errdefer ApiHttpServer.deinitJsonValue(inner_alloc, &stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_matched", .{ .integer = 1 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "stats", stats);
            try ApiHttpServer.putOwnedJsonField(inner_alloc, &root.object, "worker_retries", .{ .integer = 0 });
            return .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(inner_alloc, root) };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const session_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/api-join-job-expire-lease", .{tmp.sub_path});
    defer std.testing.allocator.free(session_path);

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{
            .session_store_path = session_path,
            .join_job_retention_ms = 0,
        }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
                .upsert_join_shuffle_lease = FakeSource.upsertJoinShuffleLease,
                .remove_join_shuffle_lease = FakeSource.removeJoinShuffleLease,
            },
        }, null, null);
        defer server.deinit();

        const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 808, null, join, parsed_hits.values, &.{}, false, 1);
        defer std.testing.allocator.free(body);
        var first = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
        defer first.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 1), fake_reads.partition_calls);
        try std.testing.expect(fake_source.lease != null);
    }

    {
        var server = try ApiHttpServer.initWithConfig(std.testing.allocator, .{
            .session_store_path = session_path,
            .join_job_retention_ms = 0,
        }, .{
            .ptr = &fake_source,
            .vtable = &.{
                .status = FakeSource.status,
                .admin_snapshot = FakeSource.adminSnapshot,
                .free_admin_snapshot = FakeSource.freeAdminSnapshot,
                .upsert_join_shuffle_lease = FakeSource.upsertJoinShuffleLease,
                .remove_join_shuffle_lease = FakeSource.removeJoinShuffleLease,
            },
        }, null, null);
        defer server.deinit();

        const body = try server.encodeJoinFinalizeRequest(std.testing.allocator, 808, null, join, parsed_hits.values, &.{}, false, 1);
        defer std.testing.allocator.free(body);
        var second = try server.executeJoinFinalizeWorkerLocal(std.testing.allocator, fake_reads.source(), 201, "customers", body);
        defer second.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(usize, 2), fake_reads.partition_calls);
        try std.testing.expectEqual(@as(?u64, 808), fake_source.removed_job_id);
        try std.testing.expect(fake_source.upsert_count >= 2);
    }
}

test "api http server distributed shuffle retries alternate worker on failure" {
    const FakeSource = struct {
        snapshot: metadata_api.AdminSnapshot,

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot.status;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.snapshot;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const FakeReads = struct {
        attempts_201: usize = 0,
        attempts_202: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .join_partition_group_local = joinPartitionGroupLocal,
                },
            };
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return error.UnsupportedOperation;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return error.UnsupportedOperation;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.UnsupportedOperation;
        }

        fn joinPartitionGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return switch (group_id) {
                201 => blk: {
                    self.attempts_201 += 1;
                    break :blk null;
                },
                202 => blk: {
                    self.attempts_202 += 1;
                    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
                    defer ApiHttpServer.deinitJsonValue(alloc, &root);
                    var hits = std.json.Value{ .array = std.json.Array.init(alloc) };
                    errdefer ApiHttpServer.deinitJsonValue(alloc, &hits);
                    try hits.array.append(try parseOwnedJsonValueAlloc(alloc, "{\"_id\":\"doc:1\",\"_source\":{\"customer_id\":\"cust:a\",\"customers.name\":\"Alice\"}}"));
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "hits", hits);
                    var stats = std.json.Value{ .object = std.json.ObjectMap.empty };
                    errdefer ApiHttpServer.deinitJsonValue(alloc, &stats);
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "left_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "right_rows_scanned", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_matched", .{ .integer = 1 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_left", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &stats.object, "rows_unmatched_right", .{ .integer = 0 });
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "stats", stats);
                    try ApiHttpServer.putOwnedJsonField(alloc, &root.object, "worker_retries", .{ .integer = 0 });
                    break :blk .{ .json = try ApiHttpServer.stringifyJsonValueAlloc(alloc, root) };
                },
                else => return error.TestUnexpectedResult,
            };
        }
    };

    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        .{ .table_id = 2, .name = "customers", .placement_role = "data" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 101, .table_id = 1, .start_key = "", .end_key = null },
        .{ .group_id = 201, .table_id = 2, .start_key = "", .end_key = "cust:n" },
        .{ .group_id = 202, .table_id = 2, .start_key = "cust:n", .end_key = null },
    };
    var fake_source = FakeSource{
        .snapshot = .{
            .status = .{ .metadata_group_id = 1, .metrics = .{} },
            .tables = tables[0..],
            .ranges = ranges[0..],
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .merged_group_statuses = &.{},
        },
    };
    var fake_reads = FakeReads{};
    var server = ApiHttpServer.init(std.testing.allocator, .{}, .{
        .ptr = &fake_source,
        .vtable = &.{
            .status = FakeSource.status,
            .admin_snapshot = FakeSource.adminSnapshot,
            .free_admin_snapshot = FakeSource.freeAdminSnapshot,
        },
    }, null, null);
    defer server.deinit();

    var parsed_hits = try parseTestQueryHitsAlloc(std.testing.allocator,
        \\[
        \\  {"_id":"doc:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(std.testing.allocator);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = @constCast("customers"),
        .join_type = .inner,
        .left_field = @constCast("customer_id"),
        .right_field = @constCast("_id"),
    };
    var result = (try server.executeSupportedDistributedJoinPartitions(std.testing.allocator, fake_reads.source(), null, join, parsed_hits.values, false, .{
        .strategy = .shuffle,
        .shuffle_partitions = 1,
    }, null)).?;
    defer result.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 1), fake_reads.attempts_201);
    try std.testing.expectEqual(@as(usize, 1), fake_reads.attempts_202);
    try std.testing.expectEqual(@as(usize, 1), result.worker_retries);
    try std.testing.expectEqual(@as(usize, 2), result.worker_attempts.len);
    try std.testing.expectEqual(@as(u64, 201), result.worker_attempts[0].worker_group_id);
    try std.testing.expect(!result.worker_attempts[0].succeeded);
    try std.testing.expectEqual(@as(u64, 202), result.worker_attempts[1].worker_group_id);
    try std.testing.expect(result.worker_attempts[1].succeeded);
}

fn unreachableStatus(_: *anyopaque) !metadata_api.MetadataStatus {
    return error.TestUnexpectedResult;
}

test "api http server join parser accepts foreign source maps" {
    const alloc = std.testing.allocator;
    const body =
        \\{"fields":["title"],"join":{"right_table":"customers","join_type":"inner","on":{"left_field":"customer_id","right_field":"_id","operator":"eq"}},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"postgres://db","postgres_table":"customers"}}}
    ;
    const parsed = (try ApiHttpServer.parseSupportedJoinRequest(alloc, body)).?;
    defer {
        var owned = parsed;
        owned.deinit(alloc);
    }

    try std.testing.expectEqualStrings("customers", parsed.join.right_table);
    try std.testing.expect(parsed.foreign_sources.contains("pg_customers"));
}

test "api http server executes foreign right join query through registry" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            const rows = try inner_alloc.alloc(std.json.Value, 2);
            rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\"}");
            rows[1] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:b\",\"name\":\"Bob\"}");
            return .{ .rows = rows, .total = 2 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 128 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, DummyForeign.factory);

    const DummyStatus = struct {
        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };

    var server = ApiHttpServer.init(alloc, .{}, .{
        .ptr = undefined,
        .vtable = &.{ .status = DummyStatus.status },
    }, null, null);
    defer server.deinit();
    server.setForeignRegistry(&registry);
    var parsed_hits = try parseTestQueryHitsAlloc(alloc,
        \\[
        \\  {"_id":"order:1","_source":{"customer_id":"cust:a"}}
        \\]
    );
    defer parsed_hits.deinit(alloc);

    const join: ApiHttpServer.SupportedJoinRequest = .{
        .right_table = try alloc.dupe(u8, "pg_customers"),
        .join_type = .inner,
        .left_field = try alloc.dupe(u8, "customer_id"),
        .right_field = try alloc.dupe(u8, "id"),
    };
    defer {
        var owned = join;
        owned.deinit(alloc);
    }

    var foreign_config: foreign_mod.PostgresConfig = .{
        .dsn = try alloc.dupe(u8, "postgres://db"),
        .postgres_table = try alloc.dupe(u8, "customers"),
        .columns = &.{},
    };
    defer foreign_config.deinit(alloc);

    const dummy_source: table_reads.TableReadSource = .{
        .ptr = undefined,
        .vtable = undefined,
    };
    var right = try server.executeForeignRightJoinQuery(alloc, dummy_source, foreign_config, join, parsed_hits.values, .{});
    defer right.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), right.hits.len);
    try std.testing.expectEqualStrings("cust:a", right.hits[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("Alice", testOwnedHitSourcePathValue(right.hits[0], "name").?.string);
}

test "api http server executes direct foreign table query through registry" {
    const alloc = std.testing.allocator;
    const c = @cImport(@cInclude("stdlib.h"));
    try std.testing.expectEqual(@as(c_int, 0), c.setenv("PG_DSN", "postgres://resolved", 1));
    defer _ = c.unsetenv("PG_DSN");

    const DummyForeign = struct {
        var last_dsn: ?[]u8 = null;

        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(_: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            const rows = try inner_alloc.alloc(std.json.Value, 2);
            rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\"}");
            rows[1] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:b\",\"name\":\"Bob\"}");
            return .{ .rows = rows, .total = 2 };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 128 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            if (last_dsn) |value| inner_alloc.free(value);
            last_dsn = try inner_alloc.dupe(u8, config.dsn);
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer {
        if (DummyForeign.last_dsn) |value| alloc.free(value);
        registry.deinit(alloc);
    }
    try registry.register(alloc, .postgres, DummyForeign.factory);

    const DummyStatus = struct {
        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };

    var server = ApiHttpServer.init(alloc, .{}, .{
        .ptr = undefined,
        .vtable = &.{ .status = DummyStatus.status },
    }, null, null);
    defer server.deinit();
    server.setForeignRegistry(&registry);
    const dummy_source: table_reads.TableReadSource = .{
        .ptr = undefined,
        .vtable = undefined,
    };

    const body =
        \\{"fields":["name"],"limit":1,"offset":2,"order_by":[{"field":"name"}],"filter_query":{"term":"active","field":"status"},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"${secret:pg_dsn}","postgres_table":"customers","columns":[{"name":"status","type":"text"}]}}}
    ;

    const json = (try server.executeForeignPublicTableQueryIfAny(alloc, dummy_source, "pg_customers", body, null, null)).?;
    defer alloc.free(json);

    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.responses.?.len);
    const response = parsed.value.responses.?[0];
    try std.testing.expectEqual(@as(?i64, 2), response.hits.?.total);
    try std.testing.expectEqual(@as(usize, 2), response.hits.?.hits.?.len);
    try std.testing.expectEqualStrings("cust:a", response.hits.?.hits.?[0]._id);
    try std.testing.expectEqualStrings("Alice", testQueryHitSourcePathValue(response.hits.?.hits.?[0], "name").?.string);
    try std.testing.expectEqualStrings("postgres://resolved", DummyForeign.last_dsn.?);
}

test "api http server executes direct foreign table aggregations through registry" {
    const alloc = std.testing.allocator;

    const DummyForeign = struct {
        fn destroy(ptr: *anyopaque, inner_alloc: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            inner_alloc.destroy(self);
        }

        fn query(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.QueryParams) !foreign_mod.QueryResult {
            _ = ptr;
            const rows = try inner_alloc.alloc(std.json.Value, 1);
            rows[0] = try parseOwnedJsonValueAlloc(inner_alloc, "{\"id\":\"cust:a\",\"name\":\"Alice\",\"version\":1}");
            return .{ .rows = rows, .total = 1 };
        }

        fn aggregate(ptr: *anyopaque, inner_alloc: std.mem.Allocator, _: foreign_mod.AggregateParams) !foreign_mod.AggregateResult {
            _ = ptr;
            const results = try inner_alloc.alloc(foreign_mod.NamedValue, 2);
            results[0] = .{
                .name = try inner_alloc.dupe(u8, "version_stats"),
                .value = try parseOwnedJsonValueAlloc(inner_alloc, "{\"count\":2,\"min\":1,\"max\":2,\"avg\":1.5,\"sum\":3}"),
            };
            results[1] = .{
                .name = try inner_alloc.dupe(u8, "name_terms"),
                .value = try parseOwnedJsonValueAlloc(inner_alloc, "[{\"key\":\"Alice\",\"doc_count\":1},{\"key\":\"Bob\",\"doc_count\":1}]"),
            };
            return .{ .results = results };
        }

        fn statistics(_: *anyopaque, _: []const u8) !foreign_mod.TableStatistics {
            return .{ .row_count = 2, .size_bytes = 128 };
        }

        fn factory(inner_alloc: std.mem.Allocator, config: foreign_mod.Config) !foreign_mod.Source {
            var owned = config;
            defer owned.deinit(inner_alloc);
            const self = try inner_alloc.create(@This());
            return .{
                .ptr = self,
                .vtable = &.{
                    .deinit = destroy,
                    .query = query,
                    .aggregate = aggregate,
                    .statistics = statistics,
                },
            };
        }
    };

    var registry = foreign_mod.Registry{};
    defer registry.deinit(alloc);
    try registry.register(alloc, .postgres, DummyForeign.factory);

    const DummyStatus = struct {
        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };

    var server = ApiHttpServer.init(alloc, .{}, .{
        .ptr = undefined,
        .vtable = &.{ .status = DummyStatus.status },
    }, null, null);
    defer server.deinit();
    server.setForeignRegistry(&registry);
    const dummy_source: table_reads.TableReadSource = .{
        .ptr = undefined,
        .vtable = undefined,
    };

    const body =
        \\{"fields":["name"],"aggregations":{"version_stats":{"type":"stats","field":"version"},"name_terms":{"type":"terms","field":"name","size":5}},"foreign_sources":{"pg_customers":{"type":"postgres","dsn":"postgres://db","postgres_table":"customers","columns":[{"name":"version","type":"bigint"},{"name":"name","type":"text"}]}}}
    ;

    const json = (try server.executeForeignPublicTableQueryIfAny(alloc, dummy_source, "pg_customers", body, null, null)).?;
    defer alloc.free(json);

    var parsed = try std.json.parseFromSlice(metadata_openapi.QueryResponses, alloc, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.responses.?.len);
    const aggregations = parsed.value.responses.?[0].aggregations.?;
    try std.testing.expectEqual(@as(usize, 2), aggregations.map.count());
    const version_stats = aggregations.map.get("version_stats").?;
    try std.testing.expectEqual(@as(?i64, 2), version_stats.count);
    try std.testing.expectEqual(@as(?f32, 1), version_stats.min);
    try std.testing.expectEqual(@as(?f32, 2), version_stats.max);
    try std.testing.expectEqual(@as(?f32, 1.5), version_stats.avg);
    try std.testing.expectEqual(@as(?f32, 3), version_stats.sum);
    const name_terms = aggregations.map.get("name_terms").?;
    try std.testing.expectEqual(@as(usize, 2), name_terms.buckets.?.len);
    try std.testing.expectEqualStrings("Alice", name_terms.buckets.?[0].key);
    try std.testing.expectEqual(@as(i64, 1), name_terms.buckets.?[0].doc_count);
}
