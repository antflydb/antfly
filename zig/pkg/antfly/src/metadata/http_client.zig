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
const metadata_api = @import("api.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_transition_state = @import("transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const raft_routes = @import("../raft/transport/routes.zig");
const http_common = @import("../raft/transport/http_common.zig");
const routes = @import("http_routes.zig");

pub const ActiveTransitionsResponse = struct {
    split: []metadata_transition_state.SplitTransitionRecord,
    merge: []metadata_transition_state.MergeTransitionRecord,
};

pub const MetadataHttpClient = struct {
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,

    pub fn init(alloc: std.mem.Allocator, executor: http_common.RequestExecutor) MetadataHttpClient {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    pub fn fetchStatus(self: *MetadataHttpClient, base_uri: []const u8) !metadata_api.MetadataStatus {
        return try self.getJsonValue(metadata_api.MetadataStatus, base_uri, routes.Routes.status);
    }

    pub fn fetchHead(self: *MetadataHttpClient, base_uri: []const u8) !metadata_api.MetadataHead {
        return try self.getJsonValue(metadata_api.MetadataHead, base_uri, routes.Routes.head);
    }

    pub fn fetchSnapshot(self: *MetadataHttpClient, base_uri: []const u8) !std.json.Parsed(metadata_api.AdminSnapshot) {
        return try self.getJson(metadata_api.AdminSnapshot, base_uri, routes.Routes.admin_snapshot);
    }

    pub fn listTableRanges(self: *MetadataHttpClient, base_uri: []const u8, table_id: u64) !std.json.Parsed([]metadata_table_manager.RangeRecord) {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{
            routes.Routes.table_ranges_prefix,
            table_id,
            routes.Routes.table_ranges_suffix,
        });
        defer self.alloc.free(path);
        return try self.getJson([]metadata_table_manager.RangeRecord, base_uri, path);
    }

    pub fn listGroupPlacement(self: *MetadataHttpClient, base_uri: []const u8, group_id: u64) !std.json.Parsed([]raft_reconciler.PlacementIntent) {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{
            routes.Routes.group_placement_prefix,
            group_id,
            routes.Routes.group_placement_suffix,
        });
        defer self.alloc.free(path);
        return try self.getJson([]raft_reconciler.PlacementIntent, base_uri, path);
    }

    pub fn listActiveTransitions(self: *MetadataHttpClient, base_uri: []const u8) !std.json.Parsed(ActiveTransitionsResponse) {
        return try self.getJson(ActiveTransitionsResponse, base_uri, routes.Routes.active_transitions);
    }

    pub fn triggerReallocate(self: *MetadataHttpClient, base_uri: []const u8) !void {
        try self.postNoContent(base_uri, routes.Routes.internal_reallocate, "");
    }

    pub fn upsertNode(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !void {
        try self.requestWithBody(base_uri, .POST, routes.Routes.internal_nodes, body, error.InvalidNodeRegistrationRequest, null, null);
    }

    pub fn requestNodeShutdown(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        node_id: u64,
        body: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{
            routes.Routes.internal_nodes_prefix,
            node_id,
            routes.Routes.internal_node_shutdown_suffix,
        });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .PUT, path, body, error.InvalidNodeShutdownRequest, error.NodeNotFound, null);
    }

    pub fn cancelNodeShutdown(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        node_id: u64,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{
            routes.Routes.internal_nodes_prefix,
            node_id,
            routes.Routes.internal_node_shutdown_suffix,
        });
        defer self.alloc.free(path);
        try self.requestNoBody(base_uri, .DELETE, path, null, null);
    }

    pub fn finalizeNodeShutdown(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        node_id: u64,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}", .{
            routes.Routes.internal_nodes_prefix,
            node_id,
        });
        defer self.alloc.free(path);
        try self.requestNoBody(base_uri, .DELETE, path, null, null);
    }

    pub fn reportNodeStatus(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !void {
        const status_route = try nodeStatusRouteForBody(self.alloc, body);
        defer self.alloc.free(status_route);
        try self.requestWithBody(base_uri, .POST, status_route, body, error.InvalidStoreStatusRequest, error.UnknownStore, null);
    }

    pub fn upsertSchemaProgress(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !void {
        try self.requestWithBody(base_uri, .POST, routes.Routes.internal_schema_progress, body, error.InvalidSchemaProgressRequest, null, null);
    }

    pub fn createTable(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
        });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .POST, path, body, error.InvalidCreateTableRequest, null, null);
    }

    pub fn dropTable(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
        });
        defer self.alloc.free(path);
        try self.requestNoBody(base_uri, .DELETE, path, error.TableNotFound, null);
    }

    pub fn updateSchema(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        schema_json: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
            routes.Routes.internal_table_schema_suffix,
        });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .PUT, path, schema_json, error.InvalidSchemaUpdateRequest, error.TableNotFound, null);
    }

    pub fn restoreTable(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
            routes.Routes.internal_table_restore_suffix,
        });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .POST, path, body, error.InvalidBackupRequest, null, error.TableAlreadyExists);
    }

    pub fn createIndex(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        index_name: []const u8,
        index_json: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
            routes.Routes.internal_table_indexes_infix,
            index_name,
        });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .PUT, path, index_json, error.InvalidCreateIndexRequest, error.TableNotFound, null);
    }

    pub fn dropIndex(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        index_name: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
            routes.Routes.internal_table_indexes_infix,
            index_name,
        });
        defer self.alloc.free(path);
        try self.requestNoBody(base_uri, .DELETE, path, error.IndexNotFound, null);
    }

    pub fn requestTableSplit(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
            routes.Routes.internal_split_suffix,
        });
        defer self.alloc.free(path);
        try self.postNoContent(base_uri, path, body);
    }

    pub fn requestTableMerge(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
            routes.Routes.internal_merge_suffix,
        });
        defer self.alloc.free(path);
        try self.postNoContent(base_uri, path, body);
    }

    fn getJson(self: *MetadataHttpClient, comptime T: type, base_uri: []const u8, path: []const u8) !std.json.Parsed(T) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try parseJson(T, self.alloc, resp.body);
    }

    fn getJsonValue(self: *MetadataHttpClient, comptime T: type, base_uri: []const u8, path: []const u8) !T {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSliceLeaky(T, self.alloc, resp.body, .{ .ignore_unknown_fields = true });
    }

    fn postNoContent(self: *MetadataHttpClient, base_uri: []const u8, path: []const u8, body: []const u8) !void {
        try self.requestWithBody(base_uri, .POST, path, body, null, null, null);
    }

    fn requestWithBody(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        method: http_common.Method,
        path: []const u8,
        body: []const u8,
        bad_request_err: ?anyerror,
        not_found_err: ?anyerror,
        conflict_err: ?anyerror,
    ) !void {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{
            .method = method,
            .uri = uri,
            .body = body,
            .content_type = "application/json",
        });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status, bad_request_err, not_found_err, conflict_err);
    }

    fn requestNoBody(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        method: http_common.Method,
        path: []const u8,
        not_found_err: ?anyerror,
        bad_request_err: ?anyerror,
    ) !void {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{
            .method = method,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status, bad_request_err, not_found_err, null);
    }

    fn executeWithRetry(self: *MetadataHttpClient, req: http_common.HttpRequest) !http_common.HttpResponse {
        var attempt: usize = 0;
        while (true) {
            return self.executor.execute(self.alloc, req) catch |err| switch (err) {
                error.HttpConnectionClosing,
                error.ConnectionResetByPeer,
                error.ConnectionRefused,
                error.BrokenPipe,
                error.EndOfStream,
                => {
                    if (attempt >= 1) return err;
                    attempt += 1;
                    continue;
                },
                else => return err,
            };
        }
    }

    fn mapStatus(status: u16, bad_request_err: ?anyerror, not_found_err: ?anyerror, conflict_err: ?anyerror) !void {
        if (status >= 200 and status < 300) return;
        if (status == 400) return bad_request_err orelse error.UnexpectedHttpStatus;
        if (status == 404) return not_found_err orelse error.UnexpectedHttpStatus;
        if (status == 409) return conflict_err orelse error.UnexpectedHttpStatus;
        if (status == 405) return error.UnsupportedOperation;
        return error.UnexpectedHttpStatus;
    }
};

fn join(alloc: std.mem.Allocator, base_uri: []const u8, path: []const u8) ![]u8 {
    return try raft_routes.Routes.join(alloc, base_uri, path);
}

fn parseJson(comptime T: type, alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
}

fn nodeStatusRouteForBody(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    const Parsed = struct {
        store_id: u64,
    };
    const parsed = try std.json.parseFromSlice(Parsed, alloc, body, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    defer parsed.deinit();
    return try std.fmt.allocPrint(alloc, "{s}{d}{s}", .{
        routes.Routes.internal_nodes_prefix,
        parsed.value.store_id,
        routes.Routes.internal_node_status_suffix,
    });
}

test "metadata http client retries transient connection close on fetch status" {
    const FlakyExecutor = struct {
        attempts: usize = 0,

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
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            self.attempts += 1;
            if (self.attempts == 1) return error.HttpConnectionClosing;
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"metadata_group_id":77,"metrics":{"rounds":0,"repairs":0,"rebalances":0,"splits":0,"merges":0},"projected_tables":0,"projected_ranges":0,"projected_placement_intents":0,"projected_split_transitions":0,"projected_merge_transitions":0,"projected_split_observations":0,"projected_merge_observations":0,"projected_schema_progress":0,"projected_restore_progress":0,"projected_snapshot_bootstrap_intents":0,"projected_backup_restore_bootstrap_intents":0,"projected_shuffle_join_leases":0,"projected_replication_source_statuses":0}
                ),
            };
        }
    };

    var flaky = FlakyExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, flaky.executor());
    const status = try client.fetchStatus("http://127.0.0.1:9000");
    try std.testing.expectEqual(@as(u64, 77), status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 2), flaky.attempts);
}

test "metadata http client round-trips server endpoints" {
    const metadata_http_server = @import("http_server.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");

    const FakeSource = struct {
        reallocate_count: usize = 0,
        split_count: usize = 0,
        merge_count: usize = 0,
        create_count: usize = 0,
        drop_count: usize = 0,
        update_schema_count: usize = 0,
        create_index_count: usize = 0,
        drop_index_count: usize = 0,
        upsert_node_count: usize = 0,
        upsert_store_count: usize = 0,
        report_store_status_count: usize = 0,

        const tables = [_]metadata_table_manager.TableRecord{
            .{ .table_id = 1, .name = "docs", .placement_role = "data" },
        };
        const ranges = [_]metadata_table_manager.RangeRecord{
            .{ .group_id = 10, .table_id = 1, .start_key = "doc:a", .end_key = "doc:m" },
            .{ .group_id = 11, .table_id = 1, .start_key = "doc:m", .end_key = "doc:z" },
        };
        const placement_peer_ids = [_]u64{2};
        const placements = [_]raft_reconciler.PlacementIntent{
            .{ .record = .{ .group_id = 10, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = placement_peer_ids[0..] },
        };
        const split_transitions = [_]metadata_transition_state.SplitTransitionRecord{
            .{ .transition_id = 9001, .source_group_id = 10, .destination_group_id = 12, .phase = .bootstrap_peer },
        };
        const merge_transitions = [_]metadata_transition_state.MergeTransitionRecord{
            .{ .transition_id = 9010, .donor_group_id = 11, .receiver_group_id = 10, .phase = .prepare },
        };
        const replication_source_statuses = [_]metadata_table_manager.ReplicationSourceStatusRecord{
            .{
                .table_id = 1,
                .source_ordinal = 0,
                .source_kind = "postgres",
                .external_table = "users",
                .cutover_mode = "slot_resumed",
                .slot_name = "slot_old",
                .publication_name = "pub_old",
                .phase = "streaming",
                .checkpoint = "lsn:0/10",
            },
        };
        const replication_source_action_hints = [_]metadata_api.ReplicationSourceActionHint{
            .{
                .table_id = 1,
                .table_name = @constCast("docs"),
                .source_ordinal = 0,
                .action = "reseed_exact_cutover",
                .reason = "existing_slot_non_exact_cutover",
                .reseed_exact_cutover_path = @constCast("/internal/v1/tables/docs/replication-sources/0/reseed-exact-cutover"),
            },
        };

        fn iface(self: *@This()) metadata_http_server.AdminSource {
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
                    .upsert_node = upsertNode,
                    .upsert_store = upsertStore,
                    .report_store_status = reportStoreStatus,
                    .trigger_reallocate = triggerReallocate,
                    .request_split = requestSplit,
                    .request_merge = requestMerge,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_tables = 1,
                .projected_ranges = 2,
                .projected_placement_intents = 1,
                .projected_split_transitions = 1,
                .projected_merge_transitions = 1,
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{
                    .metadata_group_id = 77,
                    .metrics = .{},
                    .projected_tables = 1,
                },
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast(placements[0..]),
                .split_transitions = @constCast(split_transitions[0..]),
                .merge_transitions = @constCast(merge_transitions[0..]),
                .replication_source_statuses = @constCast(replication_source_statuses[0..]),
                .replication_source_action_hints = @constCast(replication_source_action_hints[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            snapshot.* = undefined;
        }

        fn triggerReallocate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.reallocate_count += 1;
        }

        fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: @import("../api/tables.zig").CreateTableRequest) !void {
            _ = alloc;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("docs table", req.description.?);
            self.create_count += 1;
        }

        fn dropTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.drop_count += 1;
        }

        fn updateSchema(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", schema_json);
            self.update_schema_count += 1;
        }

        fn createIndex(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("embed_idx", index_name);
            try std.testing.expectEqualStrings("{\"type\":\"managed_embeddings\"}", index_json);
            self.create_index_count += 1;
        }

        fn dropIndex(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("embed_idx", index_name);
            self.drop_index_count += 1;
        }

        fn upsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
            defer metadata_table_manager.freeNode(alloc, record);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), record.node_id);
            try std.testing.expectEqualStrings("data", record.role);
            self.upsert_node_count += 1;
        }

        fn upsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
            defer metadata_table_manager.freeStore(alloc, record);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), record.store_id);
            try std.testing.expectEqual(@as(u64, 7), record.node_id);
            try std.testing.expectEqualStrings("data", record.role);
            self.upsert_store_count += 1;
        }

        fn reportStoreStatus(ptr: *anyopaque, alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) !void {
            defer alloc.free(report.health_class);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), report.store_id);
            try std.testing.expectEqualStrings("healthy", report.health_class);
            self.report_store_status_count += 1;
        }

        fn requestSplit(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, req: metadata_http_server.SplitRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:m", req.split_key);
            self.split_count += 1;
        }

        fn requestMerge(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, req: metadata_http_server.MergeRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 11), req.donor_group_id);
            try std.testing.expectEqual(@as(u64, 10), req.receiver_group_id);
            self.merge_count += 1;
        }
    };

    var source = FakeSource{};
    var server = metadata_http_server.MetadataHttpServer.init(std.heap.page_allocator, .{}, source.iface());
    var listener = std_http_listener.StdHttpListener.init(std.heap.page_allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = MetadataHttpClient.init(std.heap.page_allocator, executor.executor());

    const status = try client.fetchStatus(base_uri);
    try std.testing.expectEqual(@as(u64, 77), status.metadata_group_id);

    var snapshot = try client.fetchSnapshot(base_uri);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), snapshot.value.tables.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.value.replication_source_statuses.len);
    try std.testing.expectEqualStrings("slot_resumed", snapshot.value.replication_source_statuses[0].cutover_mode);
    try std.testing.expectEqual(@as(usize, 1), snapshot.value.replication_source_action_hints.len);
    try std.testing.expectEqualStrings("reseed_exact_cutover", snapshot.value.replication_source_action_hints[0].action);

    var ranges = try client.listTableRanges(base_uri, 1);
    defer ranges.deinit();
    try std.testing.expectEqual(@as(usize, 2), ranges.value.len);

    var placement = try client.listGroupPlacement(base_uri, 10);
    defer placement.deinit();
    try std.testing.expectEqual(@as(usize, 1), placement.value.len);

    var active = try client.listActiveTransitions(base_uri);
    defer active.deinit();
    try std.testing.expectEqual(@as(usize, 1), active.value.split.len);
    try std.testing.expectEqual(@as(usize, 1), active.value.merge.len);

    try client.triggerReallocate(base_uri);
    try client.createTable(base_uri, "docs", "{\"description\":\"docs table\"}");
    try client.updateSchema(base_uri, "docs", "{\"kind\":\"demo\"}");
    try client.createIndex(base_uri, "docs", "embed_idx", "{\"type\":\"managed_embeddings\"}");
    try client.dropIndex(base_uri, "docs", "embed_idx");
    try client.dropTable(base_uri, "docs");
    try client.upsertNode(base_uri, "{\"store_id\":7,\"node_id\":7}");
    try client.reportNodeStatus(base_uri, "{\"store_id\":7,\"health_class\":\"healthy\"}");
    try client.requestTableSplit(base_uri, "docs", "{\"split_key\":\"doc:m\"}");
    try client.requestTableMerge(base_uri, "docs", "{\"donor_group_id\":11,\"receiver_group_id\":10}");
    try std.testing.expectEqual(@as(usize, 1), source.create_count);
    try std.testing.expectEqual(@as(usize, 1), source.drop_count);
    try std.testing.expectEqual(@as(usize, 1), source.update_schema_count);
    try std.testing.expectEqual(@as(usize, 1), source.create_index_count);
    try std.testing.expectEqual(@as(usize, 1), source.drop_index_count);
    try std.testing.expectEqual(@as(usize, 1), source.upsert_node_count);
    try std.testing.expectEqual(@as(usize, 1), source.upsert_store_count);
    try std.testing.expectEqual(@as(usize, 1), source.report_store_status_count);
    try std.testing.expectEqual(@as(usize, 1), source.reallocate_count);
    try std.testing.expectEqual(@as(usize, 1), source.split_count);
    try std.testing.expectEqual(@as(usize, 1), source.merge_count);
}
