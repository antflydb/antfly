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
const extension_domain = @import("../extensions/mod.zig");
const metadata_api = @import("api.zig");
const metadata_reconciler = @import("reconciler.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_transition_state = @import("transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const raft_routes = @import("../raft/transport/routes.zig");
const http_common = @import("../raft/transport/http_common.zig");
const routes = @import("http_routes.zig");

const max_transport_retries: usize = 1;
const max_metadata_not_leader_retries: usize = 2;
const default_request_timeout_ms: u32 = 5_000;

fn isUriUnreserved(ch: u8) bool {
    return (ch >= 'A' and ch <= 'Z') or
        (ch >= 'a' and ch <= 'z') or
        (ch >= '0' and ch <= '9') or
        ch == '-' or ch == '.' or ch == '_' or ch == '~';
}

fn percentEncodePathComponent(alloc: std.mem.Allocator, value: []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (value) |ch| {
        if (isUriUnreserved(ch)) {
            try out.append(alloc, ch);
        } else {
            var buf: [3]u8 = undefined;
            const encoded = try std.fmt.bufPrint(&buf, "%{X:0>2}", .{ch});
            try out.appendSlice(alloc, encoded);
        }
    }
    return try out.toOwnedSlice(alloc);
}

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

    pub fn restoreExtensions(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !void {
        try self.requestWithBody(base_uri, .POST, routes.Routes.internal_extension_restore, body, error.InvalidExtensionRestoreRequest, null, null);
    }

    pub fn installExtension(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        name: []const u8,
        body: []const u8,
    ) !std.json.Parsed(extension_domain.InstalledExtension) {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{ routes.Routes.internal_extensions_prefix, name });
        defer self.alloc.free(path);
        return try self.requestJsonWithBody(extension_domain.InstalledExtension, base_uri, .POST, path, body, error.InvalidExtensionLifecycleRequest, error.ExtensionNotInstalled, error.ExtensionAlreadyInstalled);
    }

    pub fn updateExtension(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        name: []const u8,
        body: []const u8,
    ) !std.json.Parsed(extension_domain.InstalledExtension) {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{ routes.Routes.internal_extensions_prefix, name, routes.Routes.internal_extension_update_suffix });
        defer self.alloc.free(path);
        return try self.requestJsonWithBody(extension_domain.InstalledExtension, base_uri, .POST, path, body, error.InvalidExtensionLifecycleRequest, error.ExtensionNotInstalled, error.ExtensionAlreadyInstalled);
    }

    pub fn dropExtension(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        name: []const u8,
        body: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{ routes.Routes.internal_extensions_prefix, name, routes.Routes.internal_extension_drop_suffix });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .POST, path, body, error.InvalidExtensionLifecycleRequest, error.ExtensionNotInstalled, error.ExtensionAlreadyInstalled);
    }

    pub fn enableExtension(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        name: []const u8,
    ) !std.json.Parsed(extension_domain.InstalledExtension) {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{ routes.Routes.internal_extensions_prefix, name, routes.Routes.internal_extension_enable_suffix });
        defer self.alloc.free(path);
        return try self.requestJsonWithBody(extension_domain.InstalledExtension, base_uri, .POST, path, "{}", error.InvalidExtensionLifecycleRequest, error.ExtensionNotInstalled, error.ExtensionAlreadyInstalled);
    }

    pub fn disableExtension(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        name: []const u8,
    ) !std.json.Parsed(extension_domain.InstalledExtension) {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{ routes.Routes.internal_extensions_prefix, name, routes.Routes.internal_extension_disable_suffix });
        defer self.alloc.free(path);
        return try self.requestJsonWithBody(extension_domain.InstalledExtension, base_uri, .POST, path, "{}", error.InvalidExtensionLifecycleRequest, error.ExtensionNotInstalled, error.ExtensionAlreadyInstalled);
    }

    pub fn configureExtension(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        name: []const u8,
        body: []const u8,
    ) !std.json.Parsed(extension_domain.InstalledExtension) {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{ routes.Routes.internal_extensions_prefix, name, routes.Routes.internal_extension_config_suffix });
        defer self.alloc.free(path);
        return try self.requestJsonWithBody(extension_domain.InstalledExtension, base_uri, .PUT, path, body, error.InvalidExtensionLifecycleRequest, error.ExtensionNotInstalled, error.ExtensionAlreadyInstalled);
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

    pub fn replaceTableDefinition(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !void {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            table_name,
            routes.Routes.internal_table_definition_suffix,
        });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .PUT, path, body, error.InvalidTableDefinitionReplacement, error.TableNotFound, error.TableGenerationChanged);
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

    pub fn putArtifactEnrichment(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        enrichment_name: []const u8,
        enrichment_json: []const u8,
    ) !void {
        const escaped_table_name = try percentEncodePathComponent(self.alloc, table_name);
        defer self.alloc.free(escaped_table_name);
        const escaped_enrichment_name = try percentEncodePathComponent(self.alloc, enrichment_name);
        defer self.alloc.free(escaped_enrichment_name);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            escaped_table_name,
            routes.Routes.internal_table_enrichments_infix,
            escaped_enrichment_name,
        });
        defer self.alloc.free(path);
        try self.requestWithBody(base_uri, .PUT, path, enrichment_json, error.InvalidExtensionEnrichment, error.TableNotFound, null);
    }

    pub fn deleteArtifactEnrichment(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        enrichment_name: []const u8,
    ) !void {
        const escaped_table_name = try percentEncodePathComponent(self.alloc, table_name);
        defer self.alloc.free(escaped_table_name);
        const escaped_enrichment_name = try percentEncodePathComponent(self.alloc, enrichment_name);
        defer self.alloc.free(escaped_enrichment_name);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}", .{
            routes.Routes.internal_tables_prefix,
            escaped_table_name,
            routes.Routes.internal_table_enrichments_infix,
            escaped_enrichment_name,
        });
        defer self.alloc.free(path);
        try self.requestNoBody(base_uri, .DELETE, path, error.EnrichmentNotFound, error.InvalidExtensionEnrichment);
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
        try self.requestWithBody(base_uri, .POST, path, body, null, null, error.DocIdentityNamespaceMismatch);
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
        try self.requestWithBody(base_uri, .POST, path, body, null, null, error.DocIdentityNamespaceMismatch);
    }

    fn getJson(self: *MetadataHttpClient, comptime T: type, base_uri: []const u8, path: []const u8) !std.json.Parsed(T) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{
            .method = .GET,
            .uri = uri,
            .timeout_ms = default_request_timeout_ms,
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
            .timeout_ms = default_request_timeout_ms,
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
            .timeout_ms = default_request_timeout_ms,
        });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status, bad_request_err, not_found_err, conflict_err);
    }

    fn requestJsonWithBody(
        self: *MetadataHttpClient,
        comptime T: type,
        base_uri: []const u8,
        method: http_common.Method,
        path: []const u8,
        body: []const u8,
        bad_request_err: ?anyerror,
        not_found_err: ?anyerror,
        conflict_err: ?anyerror,
    ) !std.json.Parsed(T) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{
            .method = method,
            .uri = uri,
            .body = body,
            .content_type = "application/json",
            .timeout_ms = default_request_timeout_ms,
        });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status, bad_request_err, not_found_err, conflict_err);
        return try parseJson(T, self.alloc, resp.body);
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
            .timeout_ms = default_request_timeout_ms,
        });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status, bad_request_err, not_found_err, null);
    }

    fn executeWithRetry(self: *MetadataHttpClient, req: http_common.HttpRequest) !http_common.HttpResponse {
        var transport_attempt: usize = 0;
        var not_leader_attempt: usize = 0;
        while (true) {
            var resp = self.executor.execute(self.alloc, req) catch |err| switch (err) {
                error.HttpConnectionClosing,
                error.ConnectionResetByPeer,
                error.ConnectionRefused,
                error.BrokenPipe,
                error.EndOfStream,
                error.Timeout,
                => {
                    if (transport_attempt >= max_transport_retries) return err;
                    transport_attempt += 1;
                    continue;
                },
                else => return err,
            };
            if (!isMetadataNotLeaderResponse(resp)) return resp;
            if (not_leader_attempt >= max_metadata_not_leader_retries) {
                resp.deinit(self.alloc);
                return error.NotLeader;
            }
            not_leader_attempt += 1;
            resp.deinit(self.alloc);
        }
    }

    fn isMetadataNotLeaderResponse(resp: http_common.HttpResponse) bool {
        if (resp.status != 503) return false;
        for (resp.headers) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, http_common.metadata_not_leader_header) and
                std.ascii.eqlIgnoreCase(std.mem.trim(u8, header.value, " \t\r\n"), http_common.metadata_not_leader_value))
            {
                return true;
            }
        }
        return false;
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
            try std.testing.expectEqual(@as(?u32, default_request_timeout_ms), req.timeout_ms);
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

test "metadata http client retries bounded timeout on fetch status" {
    const TimeoutExecutor = struct {
        attempts: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expectEqual(@as(?u32, default_request_timeout_ms), req.timeout_ms);
            self.attempts += 1;
            if (self.attempts == 1) return error.Timeout;
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"metadata_group_id":88,"metrics":{"rounds":0,"repairs":0,"rebalances":0,"splits":0,"merges":0},"projected_tables":0,"projected_ranges":0,"projected_placement_intents":0,"projected_split_transitions":0,"projected_merge_transitions":0,"projected_split_observations":0,"projected_merge_observations":0,"projected_schema_progress":0,"projected_restore_progress":0,"projected_snapshot_bootstrap_intents":0,"projected_backup_restore_bootstrap_intents":0,"projected_shuffle_join_leases":0,"projected_replication_source_statuses":0}
                ),
            };
        }
    };

    var timeout_executor = TimeoutExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, timeout_executor.executor());
    const status = try client.fetchStatus("http://127.0.0.1:9000");
    try std.testing.expectEqual(@as(u64, 88), status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 2), timeout_executor.attempts);
}

test "metadata http client retries explicit metadata not leader response" {
    const NotLeaderExecutor = struct {
        attempts: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn notLeaderResponse(alloc: std.mem.Allocator) !http_common.HttpResponse {
            const headers = try alloc.alloc(http_common.Header, 1);
            var initialized_headers: usize = 0;
            errdefer {
                for (headers[0..initialized_headers]) |*header| header.deinit(alloc);
                alloc.free(headers);
            }
            var header_name: ?[]u8 = try alloc.dupe(u8, http_common.metadata_not_leader_header);
            errdefer if (header_name) |value| alloc.free(value);
            var header_value: ?[]u8 = try alloc.dupe(u8, http_common.metadata_not_leader_value);
            errdefer if (header_value) |value| alloc.free(value);
            headers[0] = .{
                .name = header_name.?,
                .value = header_value.?,
            };
            header_name = null;
            header_value = null;
            initialized_headers += 1;
            const body = try alloc.dupe(u8, "metadata leader unavailable");
            errdefer alloc.free(body);
            return .{
                .status = 503,
                .headers = headers,
                .body = body,
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            self.attempts += 1;
            if (self.attempts <= 2) return try notLeaderResponse(alloc);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"metadata_group_id":88,"metrics":{"rounds":0,"repairs":0,"rebalances":0,"splits":0,"merges":0},"projected_tables":0,"projected_ranges":0,"projected_placement_intents":0,"projected_split_transitions":0,"projected_merge_transitions":0,"projected_split_observations":0,"projected_merge_observations":0,"projected_schema_progress":0,"projected_restore_progress":0,"projected_snapshot_bootstrap_intents":0,"projected_backup_restore_bootstrap_intents":0,"projected_shuffle_join_leases":0,"projected_replication_source_statuses":0}
                ),
            };
        }
    };

    var executor = NotLeaderExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    const status = try client.fetchStatus("http://127.0.0.1:9000");
    try std.testing.expectEqual(@as(u64, 88), status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 3), executor.attempts);
}

test "metadata http client preserves split merge doc identity conflicts" {
    const ConflictExecutor = struct {
        split_calls: usize = 0,
        merge_calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            if (std.mem.endsWith(u8, req.uri, "/internal/v1/tables/docs/split")) {
                self.split_calls += 1;
            } else if (std.mem.endsWith(u8, req.uri, "/internal/v1/tables/docs/merge")) {
                self.merge_calls += 1;
            } else {
                return error.TestUnexpectedResult;
            }
            return .{
                .status = 409,
                .body = try alloc.dupe(u8, "doc identity namespace mismatch"),
            };
        }
    };

    var executor = ConflictExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());

    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        client.requestTableSplit("http://127.0.0.1:9000", "docs", "{\"split_key\":\"doc:m\"}"),
    );
    try std.testing.expectError(
        error.DocIdentityNamespaceMismatch,
        client.requestTableMerge("http://127.0.0.1:9000", "docs", "{\"donor_group_id\":11,\"receiver_group_id\":10}"),
    );
    try std.testing.expectEqual(@as(usize, 1), executor.split_calls);
    try std.testing.expectEqual(@as(usize, 1), executor.merge_calls);
}

test "metadata http client percent-encodes artifact enrichment path components" {
    const EncodingExecutor = struct {
        calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            switch (self.calls) {
                1 => {
                    try std.testing.expectEqual(http_common.Method.PUT, req.method);
                    try std.testing.expectEqualStrings("http://127.0.0.1:9000/internal/v1/tables/docs%20table/enrichments/document%20chunks%2Fv2", req.uri);
                    try std.testing.expectEqualStrings("{\"kind\":\"chunk\"}", req.body);
                    return .{ .status = 202 };
                },
                2 => {
                    try std.testing.expectEqual(http_common.Method.DELETE, req.method);
                    try std.testing.expectEqualStrings("http://127.0.0.1:9000/internal/v1/tables/docs%20table/enrichments/document%20chunks%2Fv2", req.uri);
                    return .{ .status = 204 };
                },
                else => return error.TestUnexpectedResult,
            }
        }
    };

    var executor = EncodingExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    try client.putArtifactEnrichment("http://127.0.0.1:9000", "docs table", "document chunks/v2", "{\"kind\":\"chunk\"}");
    try client.deleteArtifactEnrichment("http://127.0.0.1:9000", "docs table", "document chunks/v2");
    try std.testing.expectEqual(@as(usize, 2), executor.calls);
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
        put_artifact_enrichment_count: usize = 0,
        delete_artifact_enrichment_count: usize = 0,
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
            .{ .transition_id = 9001, .attempt_epoch = 1, .source_group_id = 10, .destination_group_id = 12, .phase = .bootstrap_peer },
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
        const merged_group_statuses = [_]metadata_reconciler.MergedGroupStatus{
            .{
                .group_id = 10,
                .doc_identity = .{
                    .namespace_table_id = 1,
                    .namespace_shard_id = 10,
                    .namespace_range_id = 10,
                    .allocated_ordinals = 1,
                    .complete = true,
                },
            },
            .{
                .group_id = 11,
                .doc_identity = .{
                    .namespace_table_id = 1,
                    .namespace_shard_id = 10,
                    .namespace_range_id = 10,
                    .allocated_ordinals = 1,
                    .complete = true,
                },
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
                    .put_artifact_enrichment = putArtifactEnrichment,
                    .delete_artifact_enrichment = deleteArtifactEnrichment,
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
                .merged_group_statuses = @constCast(merged_group_statuses[0..]),
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

        fn putArtifactEnrichment(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8, enrichment_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("document chunks/v2", enrichment_name);
            try std.testing.expectEqualStrings("{\"kind\":\"chunk\"}", enrichment_json);
            self.put_artifact_enrichment_count += 1;
        }

        fn deleteArtifactEnrichment(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, enrichment_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("document chunks/v2", enrichment_name);
            self.delete_artifact_enrichment_count += 1;
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
    try client.putArtifactEnrichment(base_uri, "docs", "document chunks/v2", "{\"kind\":\"chunk\"}");
    try client.deleteArtifactEnrichment(base_uri, "docs", "document chunks/v2");
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
    try std.testing.expectEqual(@as(usize, 1), source.put_artifact_enrichment_count);
    try std.testing.expectEqual(@as(usize, 1), source.delete_artifact_enrichment_count);
    try std.testing.expectEqual(@as(usize, 1), source.upsert_node_count);
    try std.testing.expectEqual(@as(usize, 1), source.upsert_store_count);
    try std.testing.expectEqual(@as(usize, 1), source.report_store_status_count);
    try std.testing.expectEqual(@as(usize, 1), source.reallocate_count);
    try std.testing.expectEqual(@as(usize, 1), source.split_count);
    try std.testing.expectEqual(@as(usize, 1), source.merge_count);
}

test "metadata http client round-trips range doc identity fields" {
    const RangeExecutor = struct {
        fn executor(_: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            const body =
                if (std.mem.endsWith(u8, req.uri, routes.Routes.admin_snapshot))
                    \\{"status":{"metadata_group_id":77,"metrics":{"rounds":0,"repairs":0,"rebalances":0,"splits":0,"merges":0}},"tables":[],"ranges":[{"group_id":11,"range_id":1100,"table_id":1,"doc_identity_shard_id":10,"doc_identity_range_id":1000,"start_key":"m","end_key":null}],"stores":[],"placement_intents":[],"split_transitions":[],"merge_transitions":[]}
                else if (std.mem.endsWith(u8, req.uri, "/metadata/v1/tables/1/ranges"))
                    \\[{"group_id":11,"range_id":1100,"table_id":1,"doc_identity_shard_id":10,"doc_identity_range_id":1000,"start_key":"m","end_key":null}]
                else
                    return error.TestUnexpectedResult;
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, body),
            };
        }
    };

    var executor = RangeExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());

    var snapshot = try client.fetchSnapshot("http://127.0.0.1:9000");
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), snapshot.value.ranges.len);
    try std.testing.expectEqual(@as(u64, 10), snapshot.value.ranges[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 1000), snapshot.value.ranges[0].doc_identity_range_id);

    var ranges = try client.listTableRanges("http://127.0.0.1:9000", 1);
    defer ranges.deinit();
    try std.testing.expectEqual(@as(usize, 1), ranges.value.len);
    try std.testing.expectEqual(@as(u64, 10), metadata_table_manager.rangeDocIdentityShardId(ranges.value[0]));
    try std.testing.expectEqual(@as(u64, 1000), metadata_table_manager.rangeDocIdentityRangeId(ranges.value[0]));
}

test "metadata http client parses legacy range records without doc identity fields" {
    const alloc = std.testing.allocator;
    var parsed = try parseJson([]metadata_table_manager.RangeRecord, alloc,
        \\[
        \\  {"group_id":42,"range_id":4200,"table_id":7,"start_key":"","end_key":null},
        \\  {"group_id":43,"range_id":4300,"table_id":7,"doc_identity_shard_id":42,"doc_identity_range_id":4200,"start_key":"m","end_key":null}
        \\]
    );
    defer parsed.deinit();

    try std.testing.expectEqual(@as(usize, 2), parsed.value.len);
    try std.testing.expectEqual(@as(u64, 0), parsed.value[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 0), parsed.value[0].doc_identity_range_id);
    try std.testing.expectEqual(@as(u64, 42), metadata_table_manager.rangeDocIdentityShardId(parsed.value[0]));
    try std.testing.expectEqual(@as(u64, 4200), metadata_table_manager.rangeDocIdentityRangeId(parsed.value[0]));
    try std.testing.expectEqual(@as(u64, 42), metadata_table_manager.rangeDocIdentityShardId(parsed.value[1]));
    try std.testing.expectEqual(@as(u64, 4200), metadata_table_manager.rangeDocIdentityRangeId(parsed.value[1]));
}
