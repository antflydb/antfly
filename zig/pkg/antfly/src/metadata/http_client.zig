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
const ant_json = @import("antfly-json");
const platform_time = @import("antfly_platform").time;
const internal_service_auth = @import("../api/internal_service_auth.zig");
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
// The server's read-index barrier is bounded at five seconds. A full catalog
// snapshot can still require meaningful clone/serialization time, so keep the
// transport ceiling larger while the caller's shared budget remains the hard
// end-to-end bound.
const linearizable_snapshot_request_timeout_ms: u32 = 10_000;
const default_mutation_authority_retry_delay_ns: u64 = 100 * std.time.ns_per_ms;
const max_mutation_authority_retry_delay_ns: u64 = std.time.ns_per_s;
const mutation_authority_retry_cancellation_slice_ns: u64 = 10 * std.time.ns_per_ms;

const RetryPolicy = enum {
    /// Preserve the metadata client's existing retry behavior.
    replay_safe,
    /// Retry only failures that prove the server did not admit the mutation.
    at_most_once,
};

/// One monotonic budget shared by every transport and not-leader retry in a
/// logical metadata operation. The cancellation pointer is borrowed only by
/// synchronous request execution.
pub const RequestBudget = struct {
    deadline_ns: u64,
    cancellation: ?*const http_common.RequestCancellation = null,
};

fn ensureRoutingBudget(budget: ?RequestBudget) !void {
    if (budget) |value| {
        if (value.cancellation) |signal| {
            if (signal.isCancelled()) return error.Cancelled;
        }
        if (platform_time.monotonicNs() >= value.deadline_ns) return error.CatalogRoutingSnapshotTimeout;
    }
}

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
    internal_service: ?internal_service_auth.Config = null,

    pub fn init(alloc: std.mem.Allocator, executor: http_common.RequestExecutor) MetadataHttpClient {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    pub fn withInternalServiceAuth(
        self: *MetadataHttpClient,
        secret: ?[]const u8,
        issuer: ?[]const u8,
    ) *MetadataHttpClient {
        self.internal_service = if (secret) |value|
            if (value.len > 0) .{
                .secret = value,
                .issuer = issuer orelse "antfly-node",
            } else null
        else
            null;
        return self;
    }

    pub fn fetchStatus(self: *MetadataHttpClient, base_uri: []const u8) !metadata_api.MetadataStatus {
        return try self.getJsonValue(metadata_api.MetadataStatus, base_uri, routes.Routes.status);
    }

    pub fn fetchStatusWithBudget(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        budget: RequestBudget,
    ) !metadata_api.MetadataStatus {
        return try self.getJsonValueWithBudget(metadata_api.MetadataStatus, base_uri, routes.Routes.status, budget);
    }

    pub fn fetchHead(self: *MetadataHttpClient, base_uri: []const u8) !metadata_api.MetadataHead {
        return try self.fetchHeadWithBudget(base_uri, null);
    }

    pub fn fetchCapabilities(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        budget: ?RequestBudget,
    ) !metadata_api.MetadataCapabilities {
        const uri = try join(self.alloc, base_uri, routes.Routes.capabilities);
        defer self.alloc.free(uri);
        var resp = try self.executeWithRetryBudget(.{
            .method = .GET,
            .uri = uri,
            .timeout_ms = default_request_timeout_ms,
        }, budget);
        defer resp.deinit(self.alloc);
        if (resp.status == 404 or resp.status == 405) return error.UnsupportedOperation;
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSliceLeaky(metadata_api.MetadataCapabilities, self.alloc, resp.body, .{ .ignore_unknown_fields = true });
    }

    pub fn fetchHeadWithBudget(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        budget: ?RequestBudget,
    ) !metadata_api.MetadataHead {
        return try self.getJsonValueWithBudget(metadata_api.MetadataHead, base_uri, routes.Routes.head, budget);
    }

    pub fn fetchLinearizableHead(self: *MetadataHttpClient, base_uri: []const u8) !metadata_api.MetadataHead {
        var parsed = try self.requestJsonWithBody(
            metadata_api.MetadataHead,
            base_uri,
            .POST,
            routes.Routes.internal_linearizable_head,
            "{}",
            null,
            null,
            null,
        );
        defer parsed.deinit();
        return parsed.value;
    }

    pub fn fetchLinearizableSnapshot(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        budget: ?RequestBudget,
    ) !std.json.Parsed(metadata_api.AdminSnapshot) {
        const uri = try join(self.alloc, base_uri, routes.Routes.internal_linearizable_snapshot);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetryBudget(.{
            .method = .POST,
            .uri = uri,
            .body = "{}",
            .content_type = "application/json",
            .timeout_ms = linearizable_snapshot_request_timeout_ms,
        }, budget);
        defer resp.deinit(self.alloc);
        // Unknown internal routes are the capability signal during a rolling
        // upgrade. Treat both common server spellings as unsupported.
        if (resp.status == 404 or resp.status == 405) return error.UnsupportedOperation;
        try mapStatus(resp.status, null, null, null);
        return try parseJson(metadata_api.AdminSnapshot, self.alloc, resp.body);
    }

    pub fn fetchSnapshot(self: *MetadataHttpClient, base_uri: []const u8) !std.json.Parsed(metadata_api.AdminSnapshot) {
        return try self.fetchSnapshotWithBudget(base_uri, null);
    }

    pub fn fetchSnapshotWithBudget(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        budget: ?RequestBudget,
    ) !std.json.Parsed(metadata_api.AdminSnapshot) {
        return try self.getJsonWithBudget(metadata_api.AdminSnapshot, base_uri, routes.Routes.admin_snapshot, budget);
    }

    pub fn fetchRoutingSnapshotWithBudget(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        budget: ?RequestBudget,
    ) !std.json.Parsed(metadata_api.CatalogRoutingSnapshot) {
        return try self.fetchRoutingSnapshotAt(
            base_uri,
            .GET,
            routes.Routes.routing_snapshot,
            budget,
        );
    }

    pub fn fetchLinearizableRoutingSnapshot(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        budget: ?RequestBudget,
    ) !std.json.Parsed(metadata_api.CatalogRoutingSnapshot) {
        return try self.fetchRoutingSnapshotAt(
            base_uri,
            .POST,
            routes.Routes.internal_linearizable_routing_snapshot,
            budget,
        );
    }

    fn fetchRoutingSnapshotAt(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        method: http_common.Method,
        path: []const u8,
        budget: ?RequestBudget,
    ) !std.json.Parsed(metadata_api.CatalogRoutingSnapshot) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var remaining_buf: [32]u8 = undefined;
        var headers: [1]http_common.RequestHeader = undefined;
        var request: http_common.HttpRequest = .{
            .method = method,
            .uri = uri,
            .timeout_ms = default_request_timeout_ms,
        };
        if (method == .POST) {
            request.body = "{}";
            request.content_type = "application/json";
        }
        if (budget) |value| {
            const now_ns = platform_time.monotonicNs();
            if (now_ns >= value.deadline_ns) return error.CatalogRoutingSnapshotTimeout;
            const remaining_ns = value.deadline_ns - now_ns;
            const remaining_ms = @max(
                @as(u64, 1),
                (remaining_ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms,
            );
            const encoded = try std.fmt.bufPrint(&remaining_buf, "{d}", .{remaining_ms});
            headers[0] = .{ .name = routes.routing_remaining_ms_header, .value = encoded };
            request.headers = headers[0..];
        }

        var resp = self.executeWithRetryBudget(request, budget) catch |err| switch (err) {
            error.Timeout, error.DeadlineExceeded => return error.CatalogRoutingSnapshotTimeout,
            else => return err,
        };
        defer resp.deinit(self.alloc);
        if (resp.status == 504) return error.CatalogRoutingSnapshotTimeout;
        if (resp.status == 404 or resp.status == 405) return error.UnsupportedOperation;
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        var parsed = try parseJson(metadata_api.CatalogRoutingSnapshot, self.alloc, resp.body);
        errdefer parsed.deinit();
        try ensureRoutingBudget(budget);
        return parsed;
    }

    pub fn waitForRoutingChange(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        observed_token: metadata_api.CatalogRoutingChangeToken,
        confirm_absence: bool,
        budget: RequestBudget,
    ) !std.json.Parsed(metadata_api.CatalogRoutingChangeResult) {
        const uri = try join(self.alloc, base_uri, routes.Routes.internal_routing_authority);
        defer self.alloc.free(uri);

        const now_ns = platform_time.monotonicNs();
        if (now_ns >= budget.deadline_ns) return error.CatalogRoutingSnapshotTimeout;
        const remaining_ns = budget.deadline_ns - now_ns;
        const remaining_ms = @max(
            @as(u64, 1),
            (remaining_ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms,
        );
        var remaining_buf: [32]u8 = undefined;
        const remaining = try std.fmt.bufPrint(&remaining_buf, "{d}", .{remaining_ms});
        const body = try std.json.Stringify.valueAlloc(self.alloc, metadata_api.CatalogRoutingChangeRequest{
            .observed_token = observed_token,
            .confirm_absence = confirm_absence,
        }, .{});
        defer self.alloc.free(body);
        const headers = [_]http_common.RequestHeader{
            .{ .name = routes.routing_remaining_ms_header, .value = remaining },
        };
        var resp = self.executeWithRetryBudget(.{
            .method = .POST,
            .uri = uri,
            .body = body,
            .content_type = "application/json",
            .headers = headers[0..],
            .timeout_ms = default_request_timeout_ms,
        }, budget) catch |err| switch (err) {
            error.Timeout, error.DeadlineExceeded => return error.CatalogRoutingSnapshotTimeout,
            else => return err,
        };
        defer resp.deinit(self.alloc);
        if (resp.status == 404 or resp.status == 405) return error.UnsupportedOperation;
        if (resp.status == 504) return error.CatalogRoutingSnapshotTimeout;
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        var parsed = try parseJson(metadata_api.CatalogRoutingChangeResult, self.alloc, resp.body);
        errdefer parsed.deinit();
        try ensureRoutingBudget(budget);
        return parsed;
    }

    pub fn awaitCatalogRoute(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        request: metadata_api.CatalogRouteResolveRequest,
        budget: RequestBudget,
    ) !std.json.Parsed(metadata_api.CatalogRouteResolveResult) {
        const uri = try join(self.alloc, base_uri, routes.Routes.internal_await_route);
        defer self.alloc.free(uri);
        const now_ns = platform_time.monotonicNs();
        if (now_ns >= budget.deadline_ns) return error.CatalogRoutingSnapshotTimeout;
        const remaining_ms = @max(
            @as(u64, 1),
            ((budget.deadline_ns - now_ns) +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms,
        );
        var remaining_buf: [32]u8 = undefined;
        const remaining = try std.fmt.bufPrint(&remaining_buf, "{d}", .{remaining_ms});
        const body = try std.json.Stringify.valueAlloc(self.alloc, request, .{});
        defer self.alloc.free(body);
        const headers = [_]http_common.RequestHeader{
            .{ .name = routes.routing_remaining_ms_header, .value = remaining },
        };
        var resp = self.executeWithRetryBudget(.{
            .method = .POST,
            .uri = uri,
            .body = body,
            .content_type = "application/json",
            .headers = headers[0..],
            .timeout_ms = default_request_timeout_ms,
        }, budget) catch |err| switch (err) {
            error.Timeout, error.DeadlineExceeded => return error.CatalogRoutingSnapshotTimeout,
            else => return err,
        };
        defer resp.deinit(self.alloc);
        if (resp.status == 404 or resp.status == 405) return error.UnsupportedOperation;
        if (resp.status == 504) return error.CatalogRoutingSnapshotTimeout;
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        var parsed = try parseJson(metadata_api.CatalogRouteResolveResult, self.alloc, resp.body);
        errdefer parsed.deinit();
        try ensureRoutingBudget(budget);
        return parsed;
    }

    pub fn validateCatalogPublication(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        contract: metadata_api.CatalogPublicationContract,
    ) !bool {
        const body = try std.json.Stringify.valueAlloc(self.alloc, contract, .{});
        defer self.alloc.free(body);
        self.requestWithBody(
            base_uri,
            .POST,
            routes.Routes.internal_catalog_publication_check,
            body,
            null,
            null,
            error.CatalogChanged,
        ) catch |err| switch (err) {
            error.CatalogChanged => return false,
            else => return err,
        };
        return true;
    }

    pub fn validateCatalogTablePublication(
        self: *MetadataHttpClient,
        base_uri: []const u8,
        contract: metadata_api.CatalogTablePublicationContract,
    ) !bool {
        const body = try std.json.Stringify.valueAlloc(self.alloc, contract, .{});
        defer self.alloc.free(body);
        self.requestWithBody(
            base_uri,
            .POST,
            routes.Routes.internal_catalog_table_publication_check,
            body,
            null,
            null,
            error.CatalogChanged,
        ) catch |err| switch (err) {
            error.CatalogChanged => return false,
            else => return err,
        };
        return true;
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

    /// Returns `error.ReallocationOutcomeUnknown` when transport fails after
    /// admission may have occurred. Callers must observe metadata state before
    /// deciding whether to submit a new reallocation generation.
    pub fn triggerReallocate(self: *MetadataHttpClient, base_uri: []const u8) !void {
        const uri = try join(self.alloc, base_uri, routes.Routes.internal_reallocate);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetryPolicy(.{
            .method = .POST,
            .uri = uri,
            .body = "",
            .content_type = "application/json",
            .timeout_ms = default_request_timeout_ms,
        }, null, .at_most_once);
        defer resp.deinit(self.alloc);
        if (resp.status == 503) {
            // A broad authority hint without the stronger non-admission proof
            // is not safe to replay and is not an upgrade verdict. Preserve
            // the ambiguity so callers converge through observation.
            if (isMetadataNotLeaderResponse(resp)) return error.ReallocationOutcomeUnknown;
            return error.ReallocationProtocolUpgradeRequired;
        }
        try mapStatus(resp.status, null, null, null);
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
        try self.requestNoBody(base_uri, .DELETE, path, null, null, null);
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
        try self.requestNoBody(base_uri, .DELETE, path, null, null, null);
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
        try self.requestNoBody(base_uri, .DELETE, path, error.TableNotFound, null, error.TableTransitionActive);
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
        try self.requestWithBody(base_uri, .PUT, path, schema_json, error.InvalidSchemaUpdateRequest, error.TableNotFound, error.TableTransitionActive);
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
        try self.requestWithBody(base_uri, .PUT, path, index_json, error.InvalidCreateIndexRequest, error.TableNotFound, error.TableTransitionActive);
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
        try self.requestNoBody(base_uri, .DELETE, path, error.IndexNotFound, null, error.TableTransitionActive);
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
        try self.requestWithBody(base_uri, .PUT, path, enrichment_json, error.InvalidExtensionEnrichment, error.TableNotFound, error.TableTransitionActive);
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
        try self.requestNoBody(base_uri, .DELETE, path, error.EnrichmentNotFound, error.InvalidExtensionEnrichment, error.TableTransitionActive);
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
        return try self.getJsonWithBudget(T, base_uri, path, null);
    }

    fn getJsonWithBudget(
        self: *MetadataHttpClient,
        comptime T: type,
        base_uri: []const u8,
        path: []const u8,
        budget: ?RequestBudget,
    ) !std.json.Parsed(T) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetryBudget(.{
            .method = .GET,
            .uri = uri,
            .timeout_ms = default_request_timeout_ms,
        }, budget);
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try parseJson(T, self.alloc, resp.body);
    }

    fn getJsonValue(self: *MetadataHttpClient, comptime T: type, base_uri: []const u8, path: []const u8) !T {
        return try self.getJsonValueWithBudget(T, base_uri, path, null);
    }

    fn getJsonValueWithBudget(
        self: *MetadataHttpClient,
        comptime T: type,
        base_uri: []const u8,
        path: []const u8,
        budget: ?RequestBudget,
    ) !T {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetryBudget(.{
            .method = .GET,
            .uri = uri,
            .timeout_ms = default_request_timeout_ms,
        }, budget);
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSliceLeaky(T, self.alloc, resp.body, .{ .ignore_unknown_fields = true });
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
        conflict_err: ?anyerror,
    ) !void {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{
            .method = method,
            .uri = uri,
            .timeout_ms = default_request_timeout_ms,
        });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status, bad_request_err, not_found_err, conflict_err);
    }

    fn executeWithRetry(self: *MetadataHttpClient, req: http_common.HttpRequest) !http_common.HttpResponse {
        return try self.executeWithRetryBudget(req, null);
    }

    fn executeWithRetryBudget(
        self: *MetadataHttpClient,
        req: http_common.HttpRequest,
        budget: ?RequestBudget,
    ) !http_common.HttpResponse {
        return try self.executeWithRetryPolicy(req, budget, .replay_safe);
    }

    fn executeWithRetryPolicy(
        self: *MetadataHttpClient,
        req: http_common.HttpRequest,
        budget: ?RequestBudget,
        retry_policy: RetryPolicy,
    ) !http_common.HttpResponse {
        var transport_attempt: usize = 0;
        var not_leader_attempt: usize = 0;
        while (true) {
            const controlled_req = try applyRequestBudget(req, budget);
            var resp = internal_service_auth.executeRequest(
                self.alloc,
                self.executor,
                controlled_req,
                self.internal_service,
            ) catch |err| switch (err) {
                // The operating system rejected the connection before an HTTP
                // request could reach the metadata service, so retrying cannot
                // create a second reallocation generation.
                error.ConnectionRefused => {
                    if (transport_attempt >= max_transport_retries) return err;
                    transport_attempt += 1;
                    continue;
                },
                error.HttpConnectionClosing,
                error.ConnectionResetByPeer,
                error.BrokenPipe,
                error.EndOfStream,
                error.Timeout,
                => {
                    // These failures can arrive after the server admitted the
                    // POST. Although active requests coalesce, a replay can
                    // arrive after the first generation clears and create a
                    // second pass. Preserve ambiguity for observation instead.
                    if (retry_policy == .at_most_once) return error.ReallocationOutcomeUnknown;
                    if (transport_attempt >= max_transport_retries) return err;
                    transport_attempt += 1;
                    continue;
                },
                // Preserve the established behavior for other metadata calls,
                // while treating this response-less failure as ambiguous for
                // the at-most-once reallocation mutation.
                error.ConnectionTimedOut => if (retry_policy == .at_most_once)
                    return error.ReallocationOutcomeUnknown
                else
                    return err,
                else => return err,
            };
            const retryable_authority_response = switch (retry_policy) {
                .replay_safe => isMetadataNotLeaderResponse(resp),
                .at_most_once => isMetadataMutationNotAdmittedResponse(resp),
            };
            if (!retryable_authority_response) return resp;
            if (not_leader_attempt >= max_metadata_not_leader_retries) {
                resp.deinit(self.alloc);
                return error.NotLeader;
            }
            not_leader_attempt += 1;
            const retry_delay_ns = if (retry_policy == .at_most_once)
                metadataAuthorityRetryDelayNs(resp)
            else
                0;
            resp.deinit(self.alloc);
            if (retry_delay_ns != 0) try waitBeforeMetadataMutationRetry(
                retry_delay_ns,
                budget,
                controlled_req.cancellation,
            );
        }
    }

    fn isMetadataNotLeaderResponse(resp: http_common.HttpResponse) bool {
        return responseHasHeaderValue(
            resp,
            http_common.metadata_not_leader_header,
            http_common.metadata_not_leader_value,
        );
    }

    fn isMetadataMutationNotAdmittedResponse(resp: http_common.HttpResponse) bool {
        return responseHasHeaderValue(
            resp,
            http_common.metadata_mutation_not_admitted_header,
            http_common.metadata_mutation_not_admitted_value,
        );
    }

    fn responseHasHeaderValue(resp: http_common.HttpResponse, name: []const u8, value: []const u8) bool {
        if (resp.status != 503) return false;
        for (resp.headers) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, name) and
                std.ascii.eqlIgnoreCase(std.mem.trim(u8, header.value, " \t\r\n"), value))
            {
                return true;
            }
        }
        return false;
    }

    fn metadataAuthorityRetryDelayNs(resp: http_common.HttpResponse) u64 {
        for (resp.headers) |header| {
            if (!std.ascii.eqlIgnoreCase(header.name, "Retry-After")) continue;
            const seconds = std.fmt.parseUnsigned(
                u64,
                std.mem.trim(u8, header.value, " \t\r\n"),
                10,
            ) catch break;
            return @min(
                seconds *| std.time.ns_per_s,
                max_mutation_authority_retry_delay_ns,
            );
        }
        return default_mutation_authority_retry_delay_ns;
    }

    fn waitBeforeMetadataMutationRetry(
        requested_delay_ns: u64,
        budget: ?RequestBudget,
        cancellation: ?*const http_common.RequestCancellation,
    ) !void {
        const started_ns = platform_time.monotonicNs();
        var delay_ns = requested_delay_ns;
        if (budget) |value| {
            if (started_ns >= value.deadline_ns) return error.Timeout;
            delay_ns = @min(delay_ns, value.deadline_ns - started_ns);
        }

        var remaining_ns = delay_ns;
        while (remaining_ns != 0) {
            if (cancellation) |signal| {
                if (signal.isCancelled()) return error.Cancelled;
            }
            const slice_ns = @min(remaining_ns, mutation_authority_retry_cancellation_slice_ns);
            platform_time.sleepNs(slice_ns);
            remaining_ns -= slice_ns;
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

fn applyRequestBudget(req: http_common.HttpRequest, budget: ?RequestBudget) !http_common.HttpRequest {
    const value = budget orelse return req;
    const cancellation = value.cancellation orelse req.cancellation;
    if (cancellation) |signal| {
        if (signal.isCancelled()) return error.Cancelled;
    }
    const now_ns = platform_time.monotonicNs();
    if (now_ns >= value.deadline_ns) return error.Timeout;
    const remaining_ns = value.deadline_ns - now_ns;
    const remaining_ms = (remaining_ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms;
    const bounded_ms: u32 = @intCast(@min(remaining_ms, std.math.maxInt(u32)));
    var controlled = req;
    controlled.timeout_ms = if (req.timeout_ms) |request_timeout_ms|
        @min(request_timeout_ms, bounded_ms)
    else
        bounded_ms;
    controlled.cancellation = cancellation;
    return controlled;
}

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

test "metadata routing client forwards relative deadline and preserves timeout" {
    const TimeoutExecutor = struct {
        saw_budget: bool = false,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.routing_snapshot));
            const raw = req.header(routes.routing_remaining_ms_header) orelse return error.TestExpectedDeadline;
            const remaining_ms = try std.fmt.parseUnsigned(u64, raw, 10);
            try std.testing.expect(remaining_ms > 0);
            try std.testing.expect(remaining_ms <= 1_000);
            self.saw_budget = true;
            return .{
                .status = 504,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "request deadline exceeded"),
            };
        }
    };

    var executor = TimeoutExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expectError(
        error.CatalogRoutingSnapshotTimeout,
        client.fetchRoutingSnapshotWithBudget("http://127.0.0.1:9000", .{
            .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s,
        }),
    );
    try std.testing.expect(executor.saw_budget);
}

test "metadata capability client distinguishes advertised routing from N-1 absence" {
    const Executor = struct {
        status: u16,
        calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.capabilities));
            return .{
                .status = self.status,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, if (self.status == 200)
                    "{\"catalog_routing_protocol_min\":2,\"catalog_routing_protocol_max\":2}"
                else
                    "not found"),
            };
        }
    };

    var current_executor = Executor{ .status = 200 };
    var current_client = MetadataHttpClient.init(std.testing.allocator, current_executor.executor());
    const capabilities = try current_client.fetchCapabilities("http://127.0.0.1:9000", null);
    try std.testing.expect(capabilities.supportsCatalogRouting(metadata_api.catalog_routing_protocol_current));

    var legacy_executor = Executor{ .status = 404 };
    var legacy_client = MetadataHttpClient.init(std.testing.allocator, legacy_executor.executor());
    try std.testing.expectError(
        error.UnsupportedOperation,
        legacy_client.fetchCapabilities("http://127.0.0.1:9000", null),
    );
}

test "metadata linearizable routing client uses compact internal endpoint" {
    const Executor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.internal_linearizable_routing_snapshot));
            try std.testing.expectEqualStrings("{}", req.body);
            _ = req.header(routes.routing_remaining_ms_header) orelse return error.TestExpectedDeadline;
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, "{\"metadata_group_id\":1,\"catalog_revision\":9,\"tables\":[],\"ranges\":[]}"),
            };
        }
    };

    var executor = Executor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    var result = try client.fetchLinearizableRoutingSnapshot("http://127.0.0.1:9000", .{
        .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u64, 9), result.value.catalog_revision);
}

test "metadata routing change client forwards an authority-scoped long poll" {
    const Executor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.internal_routing_authority));
            _ = req.header(routes.routing_remaining_ms_header) orelse return error.TestExpectedDeadline;
            const parsed = try std.json.parseFromSlice(metadata_api.CatalogRoutingChangeRequest, alloc, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqual(@as(u64, 7), parsed.value.observed_token.metadata_group_id);
            try std.testing.expectEqual(@as(u64, 17), parsed.value.observed_token.revision);
            try std.testing.expect(parsed.value.confirm_absence);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, "{\"token\":{\"metadata_group_id\":7,\"revision\":18},\"disposition\":\"advanced\",\"changed\":true}"),
            };
        }
    };

    var executor = Executor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    var result = try client.waitForRoutingChange(
        "http://127.0.0.1:9000",
        .{ .metadata_group_id = 7, .revision = 17 },
        true,
        .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s },
    );
    defer result.deinit();
    try std.testing.expect(result.value.changed);
    try std.testing.expectEqual(metadata_api.CatalogRoutingChangeResult.Disposition.advanced, result.value.effectiveDisposition());
    try std.testing.expectEqual(@as(u64, 18), result.value.token.revision);
}

test "metadata route authority client forwards the query and decodes a route plan" {
    const Executor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.internal_await_route));
            _ = req.header(routes.routing_remaining_ms_header) orelse return error.TestExpectedDeadline;
            const parsed = try std.json.parseFromSlice(metadata_api.CatalogRouteResolveRequest, alloc, req.body, .{});
            defer parsed.deinit();
            try std.testing.expectEqualStrings("docs", parsed.value.query.table_name);
            try std.testing.expectEqual(metadata_api.CatalogRouteSelector.key, parsed.value.query.selector);
            try std.testing.expectEqualStrings("row-17", parsed.value.query.key);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"disposition":"found","token":{"metadata_group_id":1,"revision":9},"plan":{"metadata_group_id":1,"metadata_incarnation":null,"catalog_revision":9,"table_id":7,"topology_epoch":3,"groups":[{"group_id":71,"range_id":4,"identity_namespace":{"table_id":7,"shard_id":71,"range_id":4}}]}}
                ),
            };
        }
    };

    var executor = Executor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    var result = try client.awaitCatalogRoute(
        "http://127.0.0.1:9000",
        .{ .query = .{ .table_name = "docs", .selector = .key, .key = "row-17" } },
        .{ .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s },
    );
    defer result.deinit();
    try std.testing.expectEqual(metadata_api.CatalogRouteResolveResult.Disposition.found, result.value.disposition);
    try std.testing.expectEqual(@as(u64, 71), result.value.plan.?.groups[0].group_id);
}

test "metadata http client uses the reallocation route and maps upgrade gating" {
    const UpgradeRequiredExecutor = struct {
        fn executor(_: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = undefined,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.internal_reallocate));
            return .{
                .status = 503,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "metadata voter upgrade required"),
            };
        }
    };

    var executor = UpgradeRequiredExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expectError(
        error.ReallocationProtocolUpgradeRequired,
        client.triggerReallocate("http://127.0.0.1:9000"),
    );
}

test "metadata http client does not replay reallocation after ambiguous transport failures" {
    const AmbiguousExecutor = struct {
        failure: anyerror,
        attempts: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            self.attempts += 1;
            return self.failure;
        }
    };

    const failures = [_]anyerror{
        error.HttpConnectionClosing,
        error.ConnectionResetByPeer,
        error.BrokenPipe,
        error.EndOfStream,
        error.ConnectionTimedOut,
        error.Timeout,
    };
    for (failures) |failure| {
        var executor = AmbiguousExecutor{ .failure = failure };
        var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
        try std.testing.expectError(
            error.ReallocationOutcomeUnknown,
            client.triggerReallocate("http://127.0.0.1:9000"),
        );
        try std.testing.expectEqual(@as(usize, 1), executor.attempts);
    }
}

test "metadata http client retries reallocation only before connection admission" {
    const PreAdmissionExecutor = struct {
        attempts: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            self.attempts += 1;
            if (self.attempts == 1) return error.ConnectionRefused;
            return .{ .status = 202 };
        }
    };

    var executor = PreAdmissionExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    try client.triggerReallocate("http://127.0.0.1:9000");
    try std.testing.expectEqual(@as(usize, 2), executor.attempts);
}

test "metadata http client bounds mutation authority retry-after delay" {
    var one_second = [_]http_common.Header{.{
        .name = @constCast("Retry-After"),
        .value = @constCast(" 1 "),
    }};
    try std.testing.expectEqual(
        @as(u64, std.time.ns_per_s),
        MetadataHttpClient.metadataAuthorityRetryDelayNs(.{ .status = 503, .headers = one_second[0..] }),
    );

    var excessive = [_]http_common.Header{.{
        .name = @constCast("retry-after"),
        .value = @constCast("999999999999"),
    }};
    try std.testing.expectEqual(
        max_mutation_authority_retry_delay_ns,
        MetadataHttpClient.metadataAuthorityRetryDelayNs(.{ .status = 503, .headers = excessive[0..] }),
    );

    var invalid = [_]http_common.Header{.{
        .name = @constCast("Retry-After"),
        .value = @constCast("tomorrow"),
    }};
    try std.testing.expectEqual(
        default_mutation_authority_retry_delay_ns,
        MetadataHttpClient.metadataAuthorityRetryDelayNs(.{ .status = 503, .headers = invalid[0..] }),
    );
}

test "metadata http client replays reallocation only with explicit mutation non-admission proof" {
    const ProofKind = enum { mutation_not_admitted, broad_authority_hint };
    const ProofExecutor = struct {
        proof: ProofKind,
        attempts: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn rejectionResponse(
            alloc: std.mem.Allocator,
            header_name: []const u8,
            header_value: []const u8,
        ) !http_common.HttpResponse {
            const headers = try alloc.alloc(http_common.Header, 1);
            errdefer alloc.free(headers);
            headers[0] = .{
                .name = try alloc.dupe(u8, header_name),
                .value = &.{},
            };
            errdefer headers[0].deinit(alloc);
            headers[0].value = try alloc.dupe(u8, header_value);
            const body = try alloc.dupe(u8, "metadata authority unavailable");
            errdefer alloc.free(body);
            return .{ .status = 503, .headers = headers, .body = body };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            self.attempts += 1;
            if (self.attempts > 1) return .{ .status = 202 };
            return switch (self.proof) {
                .mutation_not_admitted => try rejectionResponse(
                    alloc,
                    http_common.metadata_mutation_not_admitted_header,
                    http_common.metadata_mutation_not_admitted_value,
                ),
                .broad_authority_hint => try rejectionResponse(
                    alloc,
                    http_common.metadata_not_leader_header,
                    http_common.metadata_not_leader_value,
                ),
            };
        }
    };

    var proved = ProofExecutor{ .proof = .mutation_not_admitted };
    var proved_client = MetadataHttpClient.init(std.testing.allocator, proved.executor());
    try proved_client.triggerReallocate("http://127.0.0.1:9000");
    try std.testing.expectEqual(@as(usize, 2), proved.attempts);

    var ambiguous = ProofExecutor{ .proof = .broad_authority_hint };
    var ambiguous_client = MetadataHttpClient.init(std.testing.allocator, ambiguous.executor());
    try std.testing.expectError(
        error.ReallocationOutcomeUnknown,
        ambiguous_client.triggerReallocate("http://127.0.0.1:9000"),
    );
    try std.testing.expectEqual(@as(usize, 1), ambiguous.attempts);
}

test "metadata http client requests a non-cacheable linearizable head fence" {
    const FenceExecutor = struct {
        calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expectEqualStrings(
                "http://127.0.0.1:9000/internal/v1/catalog/linearizable-head",
                req.uri,
            );
            try ant_json.testing.expectEqualJsonText(alloc, "{}", req.body);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"metadata_group_id":91,"metadata_incarnation":"11111111111111111111111111111111","metadata_epoch":18}
                ),
            };
        }
    };

    var executor = FenceExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    const head = try client.fetchLinearizableHead("http://127.0.0.1:9000");
    try std.testing.expectEqual(@as(u64, 91), head.metadata_group_id);
    try std.testing.expectEqual(@as(u64, 18), head.metadata_epoch);
    try std.testing.expectEqual(@as(usize, 1), executor.calls);
}

test "metadata http client signs internal routes without leaking authority to public routes" {
    const CaptureExecutor = struct {
        internal_calls: usize = 0,
        public_calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var service_headers: usize = 0;
            for (req.headers) |header| {
                if (!std.ascii.eqlIgnoreCase(header.name, internal_service_auth.header_name)) continue;
                service_headers += 1;
                try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, header.value, "."));
            }
            if (internal_service_auth.requestTargetsInternalApi(req.uri)) {
                self.internal_calls += 1;
                try std.testing.expectEqual(@as(usize, 1), service_headers);
            } else {
                self.public_calls += 1;
                try std.testing.expectEqual(@as(usize, 0), service_headers);
            }
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"metadata_group_id":91,"metadata_incarnation":"11111111111111111111111111111111","metadata_epoch":18}
                ),
            };
        }
    };

    var executor = CaptureExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    _ = client.withInternalServiceAuth("metadata-client-test-service-secret", "cluster-a");
    _ = try client.fetchHead("http://127.0.0.1:9000");
    _ = try client.fetchLinearizableHead("http://127.0.0.1:9000");
    try std.testing.expectEqual(@as(usize, 1), executor.public_calls);
    try std.testing.expectEqual(@as(usize, 1), executor.internal_calls);
}

test "metadata http client fetches one bounded linearizable snapshot" {
    const FenceExecutor = struct {
        calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expectEqualStrings(
                "http://127.0.0.1:9000/internal/v1/catalog/linearizable-snapshot",
                req.uri,
            );
            try std.testing.expectEqual(@as(?u32, linearizable_snapshot_request_timeout_ms), req.timeout_ms);
            try ant_json.testing.expectEqualJsonText(alloc, "{}", req.body);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"status":{"metadata_group_id":91,"metadata_incarnation":"11111111111111111111111111111111","metadata_epoch":3,"metrics":{}},"tables":[],"ranges":[],"stores":[],"placement_intents":[],"split_transitions":[],"merge_transitions":[]}
                ),
            };
        }
    };

    var executor = FenceExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    var snapshot = try client.fetchLinearizableSnapshot("http://127.0.0.1:9000", null);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(u64, 91), snapshot.value.status.metadata_group_id);
    try std.testing.expectEqual(@as(u64, 3), snapshot.value.status.metadata_epoch);
    try std.testing.expectEqual(@as(usize, 1), executor.calls);
}

test "metadata http client treats missing linearizable snapshot route as unsupported" {
    const LegacyExecutor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: http_common.HttpRequest) !http_common.HttpResponse {
            return .{
                .status = 404,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "not found"),
            };
        }
    };

    var executor = LegacyExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expectError(
        error.UnsupportedOperation,
        client.fetchLinearizableSnapshot("http://127.0.0.1:9000", null),
    );
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
    try std.testing.expectEqual(@as(u16, 0), status.reallocation_barrier_protocol_version);
    try std.testing.expectEqual(@as(u16, 0), status.runtime_status_record_version);
    try std.testing.expectEqual(@as(usize, 2), timeout_executor.attempts);
}

test "metadata http client shares deadline and cancellation across retries" {
    const BudgetExecutor = struct {
        attempts: usize = 0,
        prior_timeout_ms: u32 = std.math.maxInt(u32),

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(req.cancellation != null);
            const timeout_ms = req.timeout_ms.?;
            try std.testing.expect(timeout_ms > 0 and timeout_ms <= 250);
            try std.testing.expect(timeout_ms <= self.prior_timeout_ms);
            self.prior_timeout_ms = timeout_ms;
            self.attempts += 1;
            if (self.attempts == 1) return error.HttpConnectionClosing;
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8,
                    \\{"metadata_group_id":91,"metadata_incarnation":"11111111111111111111111111111111","metadata_epoch":4}
                ),
            };
        }
    };

    var cancellation = http_common.RequestCancellation{};
    var executor = BudgetExecutor{};
    var client = MetadataHttpClient.init(std.testing.allocator, executor.executor());
    const head = try client.fetchHeadWithBudget("http://127.0.0.1:9000", .{
        .deadline_ns = platform_time.monotonicNs() + 250 * std.time.ns_per_ms,
        .cancellation = &cancellation,
    });
    try std.testing.expectEqual(@as(u64, 91), head.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 2), executor.attempts);

    cancellation.cancel();
    try std.testing.expectError(error.Cancelled, client.fetchHeadWithBudget("http://127.0.0.1:9000", .{
        .deadline_ns = platform_time.monotonicNs() + std.time.ns_per_s,
        .cancellation = &cancellation,
    }));
    try std.testing.expectEqual(@as(usize, 2), executor.attempts);

    var active_cancellation = http_common.RequestCancellation{};
    try std.testing.expectError(error.Timeout, client.fetchHeadWithBudget("http://127.0.0.1:9000", .{
        .deadline_ns = platform_time.monotonicNs(),
        .cancellation = &active_cancellation,
    }));
    try std.testing.expectEqual(@as(usize, 2), executor.attempts);
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
    const httpx = @import("httpx");
    const metadata_http_server = @import("http_server.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");

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
                    .routing_snapshot = routingSnapshot,
                    .free_routing_snapshot = freeRoutingSnapshot,
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

        fn routingSnapshot(_: *anyopaque, _: ?u64) !metadata_api.CatalogRoutingSnapshot {
            return .{
                .tables = @constCast(tables[0..]),
                .ranges = @constCast(ranges[0..]),
            };
        }

        fn freeRoutingSnapshot(_: *anyopaque, snapshot: *metadata_api.CatalogRoutingSnapshot) void {
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
    var server_io = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer server_io.deinit();
    var listener = httpx.Server.initWithConfig(std.heap.page_allocator, server_io.io(), .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer listener.deinit();
    try server.registerRoutes(&listener);
    try listener.bind();
    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn listen(http_server: *httpx.Server) void {
            http_server.listen() catch |err| std.debug.panic("metadata httpx test listener failed: {s}", .{@errorName(err)});
        }
    }.listen, .{&listener});
    defer {
        listener.stop();
        listener_thread.join();
    }

    const address = listener.boundAddress() orelse return error.AddressNotAvailable;
    const base_uri = try std.fmt.allocPrint(std.heap.page_allocator, "http://127.0.0.1:{d}", .{address.ip4.port});
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

    var routing_snapshot = try client.fetchRoutingSnapshotWithBudget(base_uri, null);
    defer routing_snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), routing_snapshot.value.tables.len);
    try std.testing.expectEqual(@as(usize, 2), routing_snapshot.value.ranges.len);

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
    try std.testing.expectError(error.UnsupportedOperation, client.restoreExtensions(base_uri, "{}"));
    try std.testing.expectError(error.UnsupportedOperation, client.enableExtension(base_uri, "memoryaf"));
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
