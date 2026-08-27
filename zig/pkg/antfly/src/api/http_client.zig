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
const cluster = @import("cluster.zig");
const metadata_mod = @import("../metadata/domain.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const db_api = @import("../storage/db/db.zig");
const db_mod = @import("../storage/db/mod.zig");
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
const algebraic_partials_wire = @import("algebraic_partials_wire.zig");
const distributed_stats_mod = @import("../search/distributed_stats.zig");
const routes = @import("http_routes.zig");
const raft_routes = @import("../raft/transport/routes.zig");
const txn_api = @import("distributed_txn.zig");
const txn_contract = @import("distributed_txn_contract.zig");
const table_writes_api = @import("table_writes.zig");
const test_contract_helpers = @import("test_contract_helpers.zig");
const transactions_api = @import("transactions.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const query_response = @import("query_response.zig");
const backup_contract = @import("backup_contract.zig");
const internal_service_auth = @import("internal_service_auth.zig");

const transition_control_rpc_timeout_ms: u32 = 5_000;

fn parseJsonBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
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

pub const LookupResponse = struct {
    version: ?[]u8 = null,
    body: []u8,

    pub fn deinit(self: *LookupResponse, alloc: std.mem.Allocator) void {
        if (self.version) |version| alloc.free(version);
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const ScanResponse = struct {
    body: []u8,

    pub fn deinit(self: *ScanResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const RepairCancelStateResponse = struct {
    cancel_requested: bool = false,
};

/// Semantic result of a pre-decision transaction request. Only an explicit
/// server marker enters the retryable channel; transport errors and unmarked
/// HTTP statuses cannot masquerade as proof that no mutation was proposed.
pub const TxnPreDecisionOutcome = enum {
    applied,
    not_proposed,
};

pub const QueryResponse = struct {
    content_type: ?[]u8 = null,
    identity_read_generation: ?u64 = null,
    body: []u8,

    pub fn deinit(self: *QueryResponse, alloc: std.mem.Allocator) void {
        if (self.content_type) |content_type| alloc.free(content_type);
        alloc.free(self.body);
        self.* = undefined;
    }
};

const RuntimePreflightSummaryWire = struct {
    result_refs: []const []const u8 = &.{},
    graph_query_order: []const []const u8 = &.{},
    text_indexes: []const db_mod.TextIndexEstimate = &.{},
    embedding_indexes: []const db_mod.EmbeddingIndexEstimate = &.{},
    graph_indexes: []const db_mod.GraphIndexEstimate = &.{},
    text_query_stats: []const db_mod.TextFieldStats = &.{},
    doc_id_value_count: u32 = 0,
    filter_id_count: u32 = 0,
    exclude_id_count: u32 = 0,
    numeric_range_clause_count: u32 = 0,
    term_range_clause_count: u32 = 0,
    ip_range_clause_count: u32 = 0,
    bool_field_clause_count: u32 = 0,
    geo_filter_clause_count: u32 = 0,
    positive_id_result_upper_bound: ?u32 = null,
    structured_filter_doc_count_estimate: ?u64 = null,
    structured_filter_doc_count_lower_bound: ?u64 = null,
    structured_filter_doc_count_sample_estimate: ?u64 = null,
    structured_filter_count_exact: bool = false,
    structured_filter_count_sample_size: u32 = 0,
    structured_filter_count_budget_limit: ?u64 = null,
    text_result_upper_bound: ?u32 = null,
    text_term_doc_freq_total: u64 = 0,
    corpus_doc_count_estimate: ?u64 = null,
    selectivity_lower_bound_ratio: ?f32 = null,
    selectivity_sample_ratio: ?f32 = null,
    selectivity_upper_bound_ratio: ?f32 = null,
    result_doc_upper_bound: ?u32 = null,
    result_doc_estimate: ?u32 = null,
    shard_result_window: u32 = 0,
    shard_result_window_total: u64 = 0,
    stored_projection_doc_upper_bound_total: u64 = 0,
    effective_stored_projection_doc_estimate_total: ?u64 = null,
    effective_stored_projection_doc_upper_bound_total: u64 = 0,
    rerank_doc_upper_bound: u32 = 0,
    effective_rerank_doc_estimate: ?u32 = null,
    effective_rerank_doc_upper_bound: u32 = 0,
    aggregation_may_scan_full_results: bool = false,
    aggregation_second_pass_doc_estimate: ?u32 = null,
    aggregation_second_pass_doc_upper_bound: ?u32 = null,
    shard_count: u32 = 0,
    remote_shard_count: u32 = 0,
    dense_query_count: u32 = 0,
    vector_worker_candidate_count: u32 = 0,
    vector_worker_fallback_count: u32 = 0,
    vector_worker_filter_constraint_count: u32 = 0,
    vector_worker_requires_algebraic_filter_resolution: bool = false,
    dense_effective_k_total: u64 = 0,
    dense_search_width_total: u64 = 0,
    dense_search_width_max: u32 = 0,
    dense_epsilon_max: f32 = 0,
};

const QueryPreflightRequestWire = struct {
    query_request: std.json.Value,
    max_work: u32 = 0,
};

pub const RetrievalAgentResponse = struct {
    content_type: ?[]u8 = null,
    body: []u8,

    pub fn deinit(self: *RetrievalAgentResponse, alloc: std.mem.Allocator) void {
        if (self.content_type) |content_type| alloc.free(content_type);
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const BatchResponse = struct {
    owner_allocator: ?std.mem.Allocator = null,
    status: u16,
    body: []u8,

    pub fn deinit(self: *BatchResponse, alloc: std.mem.Allocator) void {
        if (self.body.len > 0) (self.owner_allocator orelse alloc).free(self.body);
        self.* = undefined;
    }
};

pub const TransactionResponse = struct {
    status: u16,
    body: []u8,

    pub fn deinit(self: *TransactionResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const TransactionBeginResponse = struct {
    body: []u8,

    pub fn deinit(self: *TransactionBeginResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const TransactionStageResponse = struct {
    status: u16,
    body: []u8,

    pub fn deinit(self: *TransactionStageResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const TransactionSavepointResponse = struct {
    status: u16,
    body: []u8,

    pub fn deinit(self: *TransactionSavepointResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const TablesResponse = struct {
    body: []u8,

    pub fn deinit(self: *TablesResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const LoadBalancedTablesResponse = struct {
    body: []u8,
    endpoint_index: usize,
    attempts: usize,

    pub fn deinit(self: *LoadBalancedTablesResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const EmptyResponse = struct {
    pub fn deinit(_: *EmptyResponse, _: std.mem.Allocator) void {}
};

pub const ApiHttpClient = struct {
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    internal_service: ?internal_service_auth.Config = null,

    pub fn init(alloc: std.mem.Allocator, executor: http_common.RequestExecutor) ApiHttpClient {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    pub fn withInternalServiceAuth(
        self: *ApiHttpClient,
        secret: ?[]const u8,
        issuer: ?[]const u8,
    ) *ApiHttpClient {
        self.internal_service = if (secret) |value|
            if (value.len > 0) .{
                .secret = value,
                .issuer = issuer orelse "antfly-node",
            } else null
        else
            null;
        return self;
    }

    /// Execute one request, attaching node authority only when the resolved
    /// request target is inside the internal API namespace. Keeping this at the
    /// shared client boundary prevents new internal RPCs from silently omitting
    /// authentication and prevents the credential from leaking to public API
    /// requests made through the same client.
    pub fn executeRequest(self: *ApiHttpClient, request: http_common.HttpRequest) !http_common.HttpResponse {
        return internal_service_auth.executeRequest(
            self.alloc,
            self.executor,
            request,
            self.internal_service,
        );
    }

    /// Public generated operations live below `/db/v1`. Internal forwarding
    /// deliberately remains rooted on the listener, so callers continue to
    /// pass a node base URI rather than having to know which routing namespace
    /// an operation belongs to.
    fn joinRoute(self: *ApiHttpClient, base_uri: []const u8, path: []const u8) ![]u8 {
        if (std.mem.startsWith(u8, path, "/internal/v1/")) {
            return raft_routes.Routes.join(self.alloc, base_uri, path);
        }
        if (std.mem.eql(u8, path, "/db/v1") or std.mem.startsWith(u8, path, "/db/v1/")) {
            return raft_routes.Routes.join(self.alloc, base_uri, path);
        }
        // Accept both listener-root and already-canonical public API base
        // URIs. Benchmark and external clients commonly retain `/db/v1` in
        // their configured base; appending it again silently targets a
        // nonexistent `/db/v1/db/v1/...` route.
        if (std.mem.endsWith(u8, base_uri, "/db/v1")) {
            return raft_routes.Routes.join(self.alloc, base_uri, path);
        }
        const canonical_path = try std.fmt.allocPrint(self.alloc, "/db/v1{s}", .{path});
        defer self.alloc.free(canonical_path);
        return raft_routes.Routes.join(self.alloc, base_uri, canonical_path);
    }

    pub fn fetchClusterStatus(self: *ApiHttpClient, base_uri: []const u8) !std.json.Parsed(cluster.ClusterStatus) {
        const uri = try self.joinRoute(base_uri, routes.Routes.status);
        defer self.alloc.free(uri);
        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSlice(cluster.ClusterStatus, self.alloc, resp.body, .{ .allocate = .alloc_always });
    }

    pub fn fetchDataRaftBatchProtocolVersion(
        self: *ApiHttpClient,
        base_uri: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !u16 {
        const uri = try self.joinRoute(base_uri, routes.Routes.internal_capabilities);
        defer self.alloc.free(uri);
        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        const Parsed = struct { data_raft_batch_protocol_version: u16 = 0 };
        const parsed = try std.json.parseFromSlice(Parsed, self.alloc, resp.body, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        return parsed.value.data_raft_batch_protocol_version;
    }

    pub fn fetchLookup(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        key: []const u8,
        fields: ?[]const u8,
    ) !LookupResponse {
        const encoded_table_name = try percentEncodePathComponent(self.alloc, table_name);
        defer self.alloc.free(encoded_table_name);
        const encoded_key = try percentEncodePathComponent(self.alloc, key);
        defer self.alloc.free(encoded_key);
        const encoded_fields = if (fields) |field_list|
            try percentEncodePathComponent(self.alloc, field_list)
        else
            null;
        defer if (encoded_fields) |value| self.alloc.free(value);
        const path = if (fields) |_|
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}?fields={s}", .{
                routes.Routes.tables_prefix,
                encoded_table_name,
                routes.Routes.documents_marker,
                encoded_key,
                encoded_fields orelse unreachable,
            })
        else
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}", .{
                routes.Routes.tables_prefix,
                encoded_table_name,
                routes.Routes.documents_marker,
                encoded_key,
            });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;

        const version = for (resp.headers) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "X-Antfly-Version")) break try self.alloc.dupe(u8, header.value);
        } else null;
        return .{
            .version = version,
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchGroupLookup(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        fields: ?[]const u8,
    ) !LookupResponse {
        return self.fetchGroupLookupWithControl(base_uri, group_id, table_name, key, fields, "read_index", null, null);
    }

    pub fn fetchGroupLookupWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        fields: ?[]const u8,
        read_consistency: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !LookupResponse {
        // Group lookups are also used for routed derived-artifact hydration.
        // Those storage keys are binary and contain namespace bytes and NUL
        // component terminators, so every externally represented component
        // must be encoded before it enters an HTTP request target. Fields are
        // user-controlled JSON paths and must likewise remain a single query
        // value even when they contain URI delimiters.
        const encoded_table_name = try percentEncodePathComponent(self.alloc, table_name);
        defer self.alloc.free(encoded_table_name);
        const encoded_key = try percentEncodePathComponent(self.alloc, key);
        defer self.alloc.free(encoded_key);
        const encoded_consistency = try percentEncodePathComponent(self.alloc, read_consistency);
        defer self.alloc.free(encoded_consistency);
        const encoded_fields = if (fields) |field_list|
            try percentEncodePathComponent(self.alloc, field_list)
        else
            null;
        defer if (encoded_fields) |value| self.alloc.free(value);
        const suffix = if (fields) |_|
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}?fields={s}&read_consistency={s}", .{
                routes.Routes.tables_prefix,
                encoded_table_name,
                routes.Routes.documents_marker,
                encoded_key,
                encoded_fields orelse unreachable,
                encoded_consistency,
            })
        else
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}?read_consistency={s}", .{
                routes.Routes.tables_prefix,
                encoded_table_name,
                routes.Routes.documents_marker,
                encoded_key,
                encoded_consistency,
            });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408, 504 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        const version = for (resp.headers) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "X-Antfly-Version")) break try self.alloc.dupe(u8, header.value);
        } else null;
        return .{ .version = version, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchScan(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: ?[]const u8,
    ) !ScanResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.documents_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = if (body != null) "application/json" else null,
            .body = body orelse "",
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) {
            std.debug.panic("fetchQuery unexpected status={} uri={s} body={s}", .{ resp.status, uri, resp.body });
        }
        return .{
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchBackupTable(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !TablesResponse {
        return self.fetchBackupTableWithHeaders(base_uri, table_name, body, &.{}, null);
    }

    pub fn fetchBackupTableFenced(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
        fence: backup_contract.TableBackupFence,
    ) !TablesResponse {
        var metadata_group_id_buffer: [20]u8 = undefined;
        const metadata_group_id = try std.fmt.bufPrint(&metadata_group_id_buffer, "{d}", .{fence.metadata_group_id});
        var table_id_buffer: [20]u8 = undefined;
        const table_id = try std.fmt.bufPrint(&table_id_buffer, "{d}", .{fence.table_id});
        var topology_count_buffer: [20]u8 = undefined;
        const topology_count = try std.fmt.bufPrint(&topology_count_buffer, "{d}", .{fence.topology_range_count});
        const definition_digest = std.fmt.bytesToHex(fence.definition_digest, .lower);
        const topology_digest = std.fmt.bytesToHex(fence.topology_digest, .lower);
        var writer_not_after_buffer: [20]u8 = undefined;
        const writer_not_after = try std.fmt.bufPrint(
            &writer_not_after_buffer,
            "{d}",
            .{fence.writer_not_after_unix_ns orelse return error.InvalidBackupFence},
        );
        const headers = [_]http_common.RequestHeader{
            .{ .name = backup_contract.backup_fence_metadata_group_id_header, .value = metadata_group_id },
            .{ .name = backup_contract.backup_fence_metadata_incarnation_header, .value = &fence.metadata_incarnation },
            .{ .name = backup_contract.backup_fence_table_id_header, .value = table_id },
            .{ .name = backup_contract.backup_fence_definition_header, .value = &definition_digest },
            .{ .name = backup_contract.backup_fence_topology_count_header, .value = topology_count },
            .{ .name = backup_contract.backup_fence_topology_header, .value = &topology_digest },
            .{ .name = backup_contract.backup_writer_not_after_header, .value = writer_not_after },
        };
        var delivery_tracker: http_common.RequestDeliveryTracker = .{};
        return self.fetchBackupTableWithHeaders(base_uri, table_name, body, &headers, &delivery_tracker);
    }

    fn fetchBackupTableWithHeaders(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
        headers: []const http_common.RequestHeader,
        delivery_tracker: ?*http_common.RequestDeliveryTracker,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.backup_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .headers = headers,
            .content_type = "application/json",
            .body = body,
            .delivery_tracker = delivery_tracker,
        }) catch |err| {
            if (delivery_tracker) |tracker| {
                const delivery = tracker.load();
                if (delivery != .not_sent and !(delivery == .unknown and err == error.ConnectionRefused))
                    return error.BackupOutcomeAmbiguous;
            }
            return err;
        };
        defer resp.deinit(self.alloc);
        if (resp.status != 201) {
            if (resp.status == 409) {
                var conflict = ant_json.parseFromSlice(
                    struct { code: []const u8 },
                    self.alloc,
                    resp.body,
                    .{ .ignore_unknown_fields = true },
                ) catch return error.UnexpectedHttpStatus;
                defer conflict.deinit();
                if (std.mem.eql(u8, conflict.value.code, "backup_already_exists")) return error.BackupAlreadyExists;
                if (std.mem.eql(u8, conflict.value.code, "table_catalog_changed")) return error.CatalogChanged;
                if (std.mem.eql(u8, conflict.value.code, "backup_outcome_ambiguous")) return error.BackupOutcomeAmbiguous;
            }
            if (resp.status == 503) {
                var unavailable = ant_json.parseFromSlice(
                    struct { code: []const u8 },
                    self.alloc,
                    resp.body,
                    .{ .ignore_unknown_fields = true },
                ) catch return error.UnexpectedHttpStatus;
                defer unavailable.deinit();
                if (std.mem.eql(u8, unavailable.value.code, "metadata_leader_unavailable")) return error.NotLeader;
                if (std.mem.eql(u8, unavailable.value.code, "metadata_capability_unavailable")) return error.MetadataCapabilityUnavailable;
            }
            return error.UnexpectedHttpStatus;
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchRestoreTable(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.restore_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 202) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchClusterBackup(
        self: *ApiHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !TablesResponse {
        const uri = try self.joinRoute(base_uri, routes.Routes.backup);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    /// Tries each endpoint at most once, advancing only when an endpoint
    /// explicitly reports the retry-safe metadata-leader-unavailable response.
    /// Other failures are returned immediately so a POST is never replayed
    /// after an ambiguous transport or application failure.
    pub fn fetchClusterBackupFromEndpoints(
        self: *ApiHttpClient,
        base_uris: []const []const u8,
        body: []const u8,
    ) !LoadBalancedTablesResponse {
        if (base_uris.len == 0) return error.NoApiEndpoints;
        const path = routes.Routes.backup;

        for (base_uris, 0..) |base_uri, endpoint_index| {
            const uri = try self.joinRoute(base_uri, path);
            defer self.alloc.free(uri);

            var resp = try self.executeRequest(.{
                .method = .POST,
                .uri = uri,
                .content_type = "application/json",
                .body = body,
            });
            defer resp.deinit(self.alloc);
            if (resp.status == 200) {
                return .{
                    .body = try self.alloc.dupe(u8, resp.body),
                    .endpoint_index = endpoint_index,
                    .attempts = endpoint_index + 1,
                };
            }
            if (!isRetryableMetadataLeaderResponse(resp)) return error.UnexpectedHttpStatus;
        }
        return error.MetadataLeaderUnavailable;
    }

    pub fn fetchClusterRestore(
        self: *ApiHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !TablesResponse {
        const uri = try self.joinRoute(base_uri, routes.Routes.restore);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 202) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchListBackups(
        self: *ApiHttpClient,
        base_uri: []const u8,
        location: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}?location={s}", .{ routes.Routes.backups, location });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupScan(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: ?[]const u8,
    ) !ScanResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.documents_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = if (body != null) "application/json" else null,
            .body = body orelse "",
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchQuery(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.query_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{
            .content_type = if (resp.content_type) |content_type| try self.alloc.dupe(u8, content_type) else null,
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchRetrievalAgent(
        self: *ApiHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !RetrievalAgentResponse {
        const uri = try self.joinRoute(base_uri, routes.Routes.agents_retrieval);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchGroupQuery(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupQueryWithControl(base_uri, group_id, table_name, body, null, null);
    }

    pub fn fetchGroupQueryWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.query_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{
            .identity_read_generation = try parseIdentityReadGenerationHeader(resp),
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchGroupQueryPreflight(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        max_work: u32,
    ) !db_mod.RuntimePreflightSummary {
        return self.fetchGroupQueryPreflightWithControl(base_uri, group_id, table_name, body, max_work, null, null);
    }

    pub fn fetchGroupQueryPreflightWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        max_work: u32,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !db_mod.RuntimePreflightSummary {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.query_preflight_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        const preflight_body = if (max_work == 0)
            try self.alloc.dupe(u8, body)
        else blk: {
            var parsed_query_request = try std.json.parseFromSlice(std.json.Value, self.alloc, body, .{ .allocate = .alloc_always });
            defer parsed_query_request.deinit();
            break :blk try std.json.Stringify.valueAlloc(self.alloc, QueryPreflightRequestWire{
                .query_request = parsed_query_request.value,
                .max_work = max_work,
            }, .{});
        };
        defer self.alloc.free(preflight_body);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = preflight_body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            400 => return remotePreflightError(resp.body),
            404 => return remotePreflightError(resp.body),
            409 => return remotePreflightError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }

        var parsed = try std.json.parseFromSlice(RuntimePreflightSummaryWire, self.alloc, resp.body, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        const result_refs = try dupeStringSlice(self.alloc, parsed.value.result_refs);
        errdefer freeStringSlice(self.alloc, result_refs);
        const graph_query_order = try dupeStringSlice(self.alloc, parsed.value.graph_query_order);
        errdefer freeStringSlice(self.alloc, graph_query_order);
        const text_indexes = try dupeTextIndexEstimates(self.alloc, parsed.value.text_indexes);
        errdefer freeTextIndexEstimates(self.alloc, text_indexes);
        const embedding_indexes = try dupeEmbeddingIndexEstimates(self.alloc, parsed.value.embedding_indexes);
        errdefer freeEmbeddingIndexEstimates(self.alloc, embedding_indexes);
        const graph_indexes = try dupeGraphIndexEstimates(self.alloc, parsed.value.graph_indexes);
        errdefer freeGraphIndexEstimates(self.alloc, graph_indexes);
        const text_query_stats = try distributed_stats_mod.cloneTextFieldStatsSlice(self.alloc, parsed.value.text_query_stats);
        errdefer distributed_stats_mod.deinitTextFieldStats(self.alloc, text_query_stats);
        var summary: db_mod.RuntimePreflightSummary = .{
            .result_refs = result_refs,
            .graph_query_order = graph_query_order,
            .text_indexes = text_indexes,
            .embedding_indexes = embedding_indexes,
            .graph_indexes = graph_indexes,
            .text_query_stats = text_query_stats,
            .doc_id_value_count = parsed.value.doc_id_value_count,
            .filter_id_count = parsed.value.filter_id_count,
            .exclude_id_count = parsed.value.exclude_id_count,
            .numeric_range_clause_count = parsed.value.numeric_range_clause_count,
            .term_range_clause_count = parsed.value.term_range_clause_count,
            .ip_range_clause_count = parsed.value.ip_range_clause_count,
            .bool_field_clause_count = parsed.value.bool_field_clause_count,
            .geo_filter_clause_count = parsed.value.geo_filter_clause_count,
            .positive_id_result_upper_bound = parsed.value.positive_id_result_upper_bound,
            .structured_filter_doc_count_estimate = parsed.value.structured_filter_doc_count_estimate,
            .structured_filter_doc_count_lower_bound = parsed.value.structured_filter_doc_count_lower_bound,
            .structured_filter_doc_count_sample_estimate = parsed.value.structured_filter_doc_count_sample_estimate,
            .structured_filter_count_exact = parsed.value.structured_filter_count_exact,
            .structured_filter_count_sample_size = parsed.value.structured_filter_count_sample_size,
            .structured_filter_count_budget_limit = parsed.value.structured_filter_count_budget_limit,
            .text_result_upper_bound = parsed.value.text_result_upper_bound,
            .text_term_doc_freq_total = parsed.value.text_term_doc_freq_total,
            .corpus_doc_count_estimate = parsed.value.corpus_doc_count_estimate,
            .selectivity_lower_bound_ratio = parsed.value.selectivity_lower_bound_ratio,
            .selectivity_sample_ratio = parsed.value.selectivity_sample_ratio,
            .selectivity_upper_bound_ratio = parsed.value.selectivity_upper_bound_ratio,
            .result_doc_upper_bound = parsed.value.result_doc_upper_bound,
            .result_doc_estimate = parsed.value.result_doc_estimate,
            .shard_result_window = parsed.value.shard_result_window,
            .shard_result_window_total = parsed.value.shard_result_window_total,
            .stored_projection_doc_upper_bound_total = parsed.value.stored_projection_doc_upper_bound_total,
            .effective_stored_projection_doc_estimate_total = parsed.value.effective_stored_projection_doc_estimate_total,
            .effective_stored_projection_doc_upper_bound_total = parsed.value.effective_stored_projection_doc_upper_bound_total,
            .rerank_doc_upper_bound = parsed.value.rerank_doc_upper_bound,
            .effective_rerank_doc_estimate = parsed.value.effective_rerank_doc_estimate,
            .effective_rerank_doc_upper_bound = parsed.value.effective_rerank_doc_upper_bound,
            .aggregation_may_scan_full_results = parsed.value.aggregation_may_scan_full_results,
            .aggregation_second_pass_doc_estimate = parsed.value.aggregation_second_pass_doc_estimate,
            .aggregation_second_pass_doc_upper_bound = parsed.value.aggregation_second_pass_doc_upper_bound,
            .shard_count = parsed.value.shard_count,
            .remote_shard_count = parsed.value.remote_shard_count,
            .dense_query_count = parsed.value.dense_query_count,
            .vector_worker_candidate_count = parsed.value.vector_worker_candidate_count,
            .vector_worker_fallback_count = parsed.value.vector_worker_fallback_count,
            .vector_worker_filter_constraint_count = parsed.value.vector_worker_filter_constraint_count,
            .vector_worker_requires_algebraic_filter_resolution = parsed.value.vector_worker_requires_algebraic_filter_resolution,
            .dense_effective_k_total = parsed.value.dense_effective_k_total,
            .dense_search_width_total = parsed.value.dense_search_width_total,
            .dense_search_width_max = parsed.value.dense_search_width_max,
            .dense_epsilon_max = parsed.value.dense_epsilon_max,
        };
        db_mod.deriveRuntimePreflightEstimates(&summary);
        return summary;
    }

    pub fn fetchGroupJoinPartition(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupJoinPartitionWithTimeout(base_uri, group_id, table_name, body, null);
    }

    pub fn fetchGroupJoinPartitionWithTimeout(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_partition_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupJoinRows(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupJoinRowsWithTimeout(base_uri, group_id, table_name, body, null);
    }

    pub fn fetchGroupJoinRowsWithTimeout(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_rows_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupJoinUnmatched(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupJoinUnmatchedWithTimeout(base_uri, group_id, table_name, body, null);
    }

    pub fn fetchGroupJoinUnmatchedWithTimeout(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_unmatched_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupTextStats(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupTextStatsWithControl(base_uri, group_id, table_name, body, null, null);
    }

    pub fn fetchGroupTextStatsWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.text_stats_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupAlgebraicPartials(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupAlgebraicPartialsWithControl(base_uri, group_id, table_name, body, null, null);
    }

    pub fn fetchGroupAlgebraicPartialsWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.algebraic_partials_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        const response_headers = [_]http_common.RequestHeader{.{
            .name = algebraic_partials_wire.response_encoding_header,
            .value = algebraic_partials_wire.base64_v1,
        }};
        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .headers = &response_headers,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupJoinFinalize(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupJoinFinalizeWithTimeout(base_uri, group_id, table_name, body, null);
    }

    pub fn fetchGroupJoinFinalizeWithTimeout(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_finalize_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupJoinJobState(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_job_state_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupGraphExpand(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupGraphExpandWithControl(base_uri, group_id, table_name, body, null, null);
    }

    pub fn fetchGroupGraphExpandWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.graph_expand_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupGraphHydrate(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupGraphHydrateWithControl(base_uri, group_id, table_name, body, null, null);
    }

    pub fn fetchGroupGraphHydrateWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.graph_hydrate_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupGraphEdges(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupGraphEdgesWithControl(base_uri, group_id, table_name, body, null, null);
    }

    pub fn fetchGroupGraphEdgesWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.graph_edges_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
            422 => return remoteGraphEdgesError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupVectorWorker(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        return self.fetchGroupVectorWorkerWithControl(base_uri, group_id, table_name, body, null, null);
    }

    pub fn fetchGroupVectorWorkerWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        cancellation: ?*const http_common.RequestCancellation,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.vector_worker_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .timeout_ms = timeout_ms,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            408 => return error.Timeout,
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{
            .identity_read_generation = try parseIdentityReadGenerationHeader(resp),
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchBatch(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !BatchResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.batch_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            201, 202 => {},
            else => return remotePublicBatchError(resp.status, resp.body),
        }
        return .{
            .status = resp.status,
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchTransactionCommit(
        self: *ApiHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !TransactionResponse {
        const uri = try self.joinRoute(base_uri, routes.Routes.transactions_commit);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 409 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{
            .status = resp.status,
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchTransactionBegin(
        self: *ApiHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !TransactionBeginResponse {
        const uri = try self.joinRoute(base_uri, routes.Routes.transactions_begin);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 201) {
            return error.UnexpectedHttpStatus;
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionSessions(self: *ApiHttpClient, base_uri: []const u8) !TransactionResponse {
        const uri = try self.joinRoute(base_uri, routes.Routes.transactions);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 404 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionSessionCleanup(self: *ApiHttpClient, base_uri: []const u8, cutoff_ns: ?u64) !TransactionResponse {
        const path = if (cutoff_ns) |cutoff|
            try std.fmt.allocPrint(self.alloc, "{s}?cutoff_ns={d}", .{ routes.Routes.transactions_cleanup, cutoff })
        else
            try self.alloc.dupe(u8, routes.Routes.transactions_cleanup);
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 400 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionSessionCommit(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
        body: []const u8,
    ) !TransactionResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.transactions_prefix,
            txn_id_hex,
            routes.Routes.transactions_commit_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 404, 409 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionSessionInfo(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
    ) !TransactionResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{
            routes.Routes.transactions_prefix,
            txn_id_hex,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 400, 404 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionSessionStage(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
        body: []const u8,
    ) !TransactionStageResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.transactions_prefix,
            txn_id_hex,
            routes.Routes.transactions_stage_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 400, 404 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionSessionRead(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
        body: []const u8,
    ) !TransactionStageResponse {
        return try self.fetchTransactionSessionStageWithSuffix(base_uri, txn_id_hex, routes.Routes.transactions_read_suffix, body);
    }

    pub fn fetchTransactionSessionWrite(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
        body: []const u8,
    ) !TransactionStageResponse {
        return try self.fetchTransactionSessionStageWithSuffix(base_uri, txn_id_hex, routes.Routes.transactions_write_suffix, body);
    }

    pub fn fetchTransactionSessionDelete(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
        body: []const u8,
    ) !TransactionStageResponse {
        return try self.fetchTransactionSessionStageWithSuffix(base_uri, txn_id_hex, routes.Routes.transactions_delete_suffix, body);
    }

    pub fn fetchTransactionSessionSavepoint(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
    ) !TransactionSavepointResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.transactions_prefix,
            txn_id_hex,
            routes.Routes.transactions_savepoints_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 404 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionSessionRollback(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
        savepoint_id: u64,
    ) !TransactionSavepointResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}/{d}{s}", .{
            routes.Routes.transactions_prefix,
            txn_id_hex,
            routes.Routes.transactions_savepoints_suffix,
            savepoint_id,
            routes.Routes.transactions_rollback_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 404 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTransactionAbort(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
    ) !TransactionResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.transactions_prefix,
            txn_id_hex,
            routes.Routes.transactions_abort_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 404 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    fn fetchTransactionSessionStageWithSuffix(
        self: *ApiHttpClient,
        base_uri: []const u8,
        txn_id_hex: []const u8,
        suffix: []const u8,
        body: []const u8,
    ) !TransactionStageResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.transactions_prefix,
            txn_id_hex,
            suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 400, 404 => {},
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .status = resp.status, .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupBatch(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !BatchResponse {
        return self.fetchGroupBatchWithTimeout(base_uri, group_id, table_name, body, null);
    }

    pub fn fetchGroupBatchWithTimeout(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !BatchResponse {
        return self.fetchGroupBatchWithForwarding(base_uri, group_id, table_name, body, timeout_ms, null, null);
    }

    pub fn fetchGroupBatchWithForwarding(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
        forwarding: ?internal_batch_forwarding.Context,
        cancellation: ?*const http_common.RequestCancellation,
    ) !BatchResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            if (forwarding != null) routes.Routes.routed_batch_suffix else routes.Routes.batch_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var remaining_buf: [10]u8 = undefined;
        var forwards_buf: [3]u8 = undefined;
        var request_headers: [3]http_common.RequestHeader = undefined;
        var header_count: usize = 0;
        if (forwarding) |context| {
            request_headers[header_count] = .{
                .name = internal_batch_forwarding.remaining_ms_header,
                .value = try std.fmt.bufPrint(&remaining_buf, "{d}", .{context.remaining_ms}),
            };
            header_count += 1;
            request_headers[header_count] = .{
                .name = internal_batch_forwarding.forwards_remaining_header,
                .value = try std.fmt.bufPrint(&forwards_buf, "{d}", .{context.forwards_remaining}),
            };
            header_count += 1;
            request_headers[header_count] = .{
                .name = internal_batch_forwarding.campaign_allowed_header,
                .value = if (context.campaign_allowed) "true" else "false",
            };
            header_count += 1;
        }
        const headers = request_headers[0..header_count];

        var delivery_tracker: http_common.RequestDeliveryTracker = .{};
        var resp = self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .headers = headers,
            .content_type = "application/json",
            .timeout_ms = timeout_ms,
            .body = body,
            .cancellation = cancellation,
            .delivery_tracker = if (forwarding != null) &delivery_tracker else null,
        }) catch |err| {
            const delivery = delivery_tracker.load();
            if (forwarding != null and delivery != .not_sent and
                !(delivery == .unknown and err == error.ConnectionRefused))
            {
                // An executor that cannot identify its send boundary remains
                // unknown, except for a refused connection that never existed.
                // An explicit post-send phase always wins over the error name.
                return error.RaftBatchWriteOutcomeUnknown;
            }
            return err;
        };
        defer resp.deinit(self.alloc);
        if (resp.status != 201) {
            const outcome = resp.header(internal_batch_forwarding.outcome_header);
            if (resp.status == 202) {
                const outcome_body = std.mem.trim(u8, resp.body, " \t\r\n");
                if ((outcome != null and std.mem.eql(u8, outcome.?, internal_batch_forwarding.outcome_committed_repair_required_v1)) or
                    std.mem.eql(u8, outcome_body, internal_batch_forwarding.committed_repair_required_body))
                    return error.EnrichmentWorkerFailed;
                if ((outcome != null and std.mem.eql(u8, outcome.?, internal_batch_forwarding.outcome_committed_visibility_pending_v1)) or
                    std.mem.eql(u8, outcome_body, internal_batch_forwarding.committed_visibility_pending_body))
                    return error.EnrichmentRetryInProgress;
            }
            if ((outcome != null and std.mem.eql(u8, outcome.?, internal_batch_forwarding.outcome_unknown_v1)) or
                std.mem.eql(u8, std.mem.trim(u8, resp.body, " \t\r\n"), "write outcome unknown"))
            {
                return error.RaftBatchWriteOutcomeUnknown;
            }
            if (resp.status == 409) return remoteGroupConflictError(resp.body);
            if (resp.status == 404) return if (forwarding != null) error.RaftBatchForwardingUnsupported else error.UnknownGroup;
            if (resp.status == 503) {
                if (forwarding == null or (outcome != null and
                    std.mem.eql(u8, outcome.?, internal_batch_forwarding.outcome_not_proposed_v1)))
                {
                    return error.LeaderUnavailable;
                }
                return error.RaftBatchWriteOutcomeUnknown;
            }
            const preview = resp.body[0..@min(resp.body.len, 256)];
            std.log.warn("internal group batch returned unexpected status={} uri={s} body={s}", .{ resp.status, uri, preview });
            return error.UnexpectedHttpStatus;
        }
        const response = BatchResponse{
            .owner_allocator = resp.owner_allocator orelse self.alloc,
            .status = resp.status,
            .body = resp.body,
        };
        resp.body = &.{};
        return response;
    }

    pub fn fetchGroupDocumentArtifactManifest(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !QueryResponse {
        const escaped_key = try percentEncodePathComponent(self.alloc, doc_key);
        defer self.alloc.free(escaped_key);
        const escaped_artifact_name = try percentEncodePathComponent(self.alloc, artifact_name);
        defer self.alloc.free(escaped_artifact_name);
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.documents_marker,
            escaped_key,
            routes.Routes.artifacts_marker,
            escaped_artifact_name,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupDocumentArtifactManifests(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
    ) !QueryResponse {
        const escaped_key = try percentEncodePathComponent(self.alloc, doc_key);
        defer self.alloc.free(escaped_key);
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.documents_marker,
            escaped_key,
            routes.Routes.artifacts_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            503 => return remoteStorageReadUnavailableError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupDocumentArtifactReprocess(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !QueryResponse {
        const escaped_key = try percentEncodePathComponent(self.alloc, doc_key);
        defer self.alloc.free(escaped_key);
        const escaped_artifact_name = try percentEncodePathComponent(self.alloc, artifact_name);
        defer self.alloc.free(escaped_artifact_name);
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.documents_marker,
            escaped_key,
            routes.Routes.artifacts_marker,
            escaped_artifact_name,
            routes.Routes.reprocess_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = "{}",
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 202 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupDocumentArtifactRangeReprocess(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const escaped_artifact_name = try percentEncodePathComponent(self.alloc, artifact_name);
        defer self.alloc.free(escaped_artifact_name);
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.artifacts_marker,
            escaped_artifact_name,
            routes.Routes.reprocess_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 202 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupArtifactRepairIssues(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.artifact_repair_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupArtifactRepairRun(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.artifact_repair_run_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 202 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            503 => if (std.mem.eql(u8, resp.body, "repair cancel unavailable")) return error.RepairCancelUnavailable else return error.UnexpectedHttpStatus,
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchTableRepairCancelRequested(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        job_id: u64,
        attempt_id: u64,
    ) !bool {
        const escaped_table_name = try percentEncodePathComponent(self.alloc, table_name);
        defer self.alloc.free(escaped_table_name);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{d}{s}{d}{s}", .{
            routes.Routes.internal_tables_prefix,
            escaped_table_name,
            routes.Routes.repair_jobs_marker,
            job_id,
            routes.Routes.repair_attempts_marker,
            attempt_id,
            routes.Routes.repair_cancel_state_suffix,
        });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {
                var parsed = try parseJsonBody(RepairCancelStateResponse, self.alloc, resp.body);
                defer parsed.deinit();
                return parsed.value.cancel_requested;
            },
            404 => return true,
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupDocumentArtifactChildRangePlacementUpdate(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const escaped_key = try percentEncodePathComponent(self.alloc, doc_key);
        defer self.alloc.free(escaped_key);
        const escaped_artifact_name = try percentEncodePathComponent(self.alloc, artifact_name);
        defer self.alloc.free(escaped_artifact_name);
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.documents_marker,
            escaped_key,
            routes.Routes.artifacts_marker,
            escaped_artifact_name,
            routes.Routes.placement_update_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 202 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupDocumentArtifactChildRangeBatchApply(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const escaped_key = try percentEncodePathComponent(self.alloc, doc_key);
        defer self.alloc.free(escaped_key);
        const escaped_artifact_name = try percentEncodePathComponent(self.alloc, artifact_name);
        defer self.alloc.free(escaped_artifact_name);
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.documents_marker,
            escaped_key,
            routes.Routes.artifacts_marker,
            escaped_artifact_name,
            routes.Routes.child_range_batch_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200, 202 => return .{ .body = try self.alloc.dupe(u8, resp.body) },
            404 => return error.NotFound,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupTxnBegin(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !EmptyResponse {
        return try self.fetchGroupTxnBeginWithDeliveryTracking(
            base_uri,
            group_id,
            table_name,
            body,
            null,
        );
    }

    pub fn fetchGroupTxnBeginWithDeliveryTracking(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        delivery_tracker: ?*http_common.RequestDeliveryTracker,
    ) !EmptyResponse {
        return switch (try self.fetchGroupTxnBeginOutcomeWithDeliveryTracking(
            base_uri,
            group_id,
            table_name,
            body,
            delivery_tracker,
            null,
            null,
        )) {
            .applied => .{},
            .not_proposed => error.GroupLeaderUnavailable,
        };
    }

    pub fn fetchGroupTxnBeginOutcomeWithDeliveryTracking(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        delivery_tracker: ?*http_common.RequestDeliveryTracker,
        timeout_ms: ?u32,
        server_budget_ms: ?u32,
    ) !TxnPreDecisionOutcome {
        // Establish the strongest safe default before URI construction,
        // request signing, or any other client-local allocation can fail.
        // The executor advances this state at its actual send boundary.
        if (delivery_tracker) |tracker| tracker.markNotSent();
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.txn_begin_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var budget_buf: [10]u8 = undefined;
        const headers: []const http_common.RequestHeader = if (server_budget_ms) |budget| blk: {
            if (budget == 0 or budget > txn_contract.max_pre_decision_server_budget_ms)
                return error.InvalidArgument;
            const value = try std.fmt.bufPrint(&budget_buf, "{d}", .{budget});
            break :blk &.{.{
                .name = txn_contract.pre_decision_remaining_ms_header,
                .value = value,
            }};
        } else &.{};
        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .headers = headers,
            .delivery_tracker = delivery_tracker,
            .timeout_ms = timeout_ms,
        });
        defer resp.deinit(self.alloc);
        // Receiving any response proves that the request crossed the send
        // boundary, even when a custom executor does not update the tracker.
        if (delivery_tracker) |tracker| tracker.markMayHaveBeenSent();
        if (isTxnPreDecisionNotProposedResponse(resp)) return .not_proposed;
        switch (resp.status) {
            200 => return .applied,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupTxnPrepare(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !EmptyResponse {
        return try self.fetchGroupTxnPrepareWithDeliveryTracking(
            base_uri,
            group_id,
            table_name,
            body,
            null,
        );
    }

    pub fn fetchGroupTxnPrepareWithDeliveryTracking(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        delivery_tracker: ?*http_common.RequestDeliveryTracker,
    ) !EmptyResponse {
        return switch (try self.fetchGroupTxnPrepareOutcomeWithDeliveryTracking(
            base_uri,
            group_id,
            table_name,
            body,
            delivery_tracker,
            null,
            null,
        )) {
            .applied => .{},
            .not_proposed => error.GroupLeaderUnavailable,
        };
    }

    pub fn fetchGroupTxnPrepareOutcomeWithDeliveryTracking(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        delivery_tracker: ?*http_common.RequestDeliveryTracker,
        timeout_ms: ?u32,
        server_budget_ms: ?u32,
    ) !TxnPreDecisionOutcome {
        // Match begin's delivery contract so callers can distinguish local
        // request construction failures from an ambiguous transmitted write.
        if (delivery_tracker) |tracker| tracker.markNotSent();
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.txn_prepare_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var budget_buf: [10]u8 = undefined;
        const headers: []const http_common.RequestHeader = if (server_budget_ms) |budget| blk: {
            if (budget == 0 or budget > txn_contract.max_pre_decision_server_budget_ms)
                return error.InvalidArgument;
            const value = try std.fmt.bufPrint(&budget_buf, "{d}", .{budget});
            break :blk &.{.{
                .name = txn_contract.pre_decision_remaining_ms_header,
                .value = value,
            }};
        } else &.{};
        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .headers = headers,
            .delivery_tracker = delivery_tracker,
            .timeout_ms = timeout_ms,
        });
        defer resp.deinit(self.alloc);
        if (delivery_tracker) |tracker| tracker.markMayHaveBeenSent();
        if (isTxnPreDecisionNotProposedResponse(resp)) return .not_proposed;
        switch (resp.status) {
            200 => return .applied,
            409 => return remoteGroupTxnPrepareConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchGroupTxnResolve(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !EmptyResponse {
        return try self.fetchGroupTxnResolveWithControl(base_uri, group_id, table_name, body, null);
    }

    pub fn fetchGroupTxnResolveWithControl(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        cancellation: ?*const http_common.RequestCancellation,
    ) !EmptyResponse {
        return try fetchInternalPostEmpty(self, base_uri, group_id, table_name, routes.Routes.txn_resolve_suffix, body, cancellation);
    }

    pub fn fetchGroupTxnAcknowledge(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !EmptyResponse {
        return try fetchInternalPostEmpty(self, base_uri, group_id, table_name, routes.Routes.txn_acknowledge_suffix, body, null);
    }

    pub fn fetchGroupTxnStatus(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.txn_status_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            409 => return remoteGroupConflictError(resp.body),
            503 => return error.GroupLeaderUnavailable,
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupShardObserveSplit(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        record: metadata_transition_state.SplitTransitionRecord,
    ) !metadata_transition_state.SplitObservation {
        const body = try jsonStringifyAlloc(self.alloc, record);
        defer self.alloc.free(body);
        const response_body = try fetchInternalGroupPost(self, base_uri, group_id, routes.Routes.shard_ops_observe_split_suffix, body);
        defer self.alloc.free(response_body);
        var parsed = try std.json.parseFromSlice(metadata_transition_state.SplitObservation, self.alloc, response_body, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        return parsed.value;
    }

    pub fn fetchGroupDbMedianKey(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
    ) !?[]u8 {
        const response_body = try fetchInternalGroupGet(self, base_uri, group_id, routes.Routes.group_db_median_key_suffix);
        defer self.alloc.free(response_body);
        const Response = struct {
            median_key: ?[]const u8 = null,
        };
        var parsed = try std.json.parseFromSlice(Response, self.alloc, response_body, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        return if (parsed.value.median_key) |median_key|
            try self.alloc.dupe(u8, median_key)
        else
            null;
    }

    pub fn fetchGroupShardObserveMerge(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        record: metadata_transition_state.MergeTransitionRecord,
    ) !metadata_transition_state.MergeObservation {
        const body = try jsonStringifyAlloc(self.alloc, record);
        defer self.alloc.free(body);
        const response_body = try fetchInternalGroupPost(self, base_uri, group_id, routes.Routes.shard_ops_observe_merge_suffix, body);
        defer self.alloc.free(response_body);
        var parsed = try std.json.parseFromSlice(metadata_transition_state.MergeObservation, self.alloc, response_body, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        return parsed.value;
    }

    pub fn fetchGroupShardExecute(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        action: metadata_mod.TransitionAction,
    ) !EmptyResponse {
        const body = try encodeTransitionAction(self.alloc, action);
        defer self.alloc.free(body);
        const response_body = try fetchInternalGroupPost(self, base_uri, group_id, routes.Routes.shard_ops_execute_suffix, body);
        self.alloc.free(response_body);
        return .{};
    }

    fn fetchInternalPostEmpty(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        suffix_name: []const u8,
        body: []const u8,
        cancellation: ?*const http_common.RequestCancellation,
    ) !EmptyResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            suffix_name,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
            .cancellation = cancellation,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return .{},
            202 => {
                if (std.mem.eql(u8, resp.body, "committed_repair_required"))
                    return error.EnrichmentWorkerFailed;
                if (std.mem.eql(u8, resp.body, "committed_visibility_pending"))
                    return error.CommitVisibilityNotSatisfied;
                return error.UnexpectedHttpStatus;
            },
            404 => return error.UnknownGroup,
            405 => return error.UnsupportedOperation,
            409 => return remoteGroupTxnResolveConflictError(resp.body),
            503 => return error.GroupLeaderUnavailable,
            else => return error.UnexpectedHttpStatus,
        }
    }

    fn fetchInternalGroupPost(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        suffix_name: []const u8,
        body: []const u8,
    ) ![]u8 {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            // Transition RPCs run from a persistent control loop. A peer that
            // accepts a connection but never responds must not monopolize that
            // loop indefinitely; TransitionService retries idempotent actions
            // with bounded backoff after this deadline.
            .timeout_ms = transition_control_rpc_timeout_ms,
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return try self.alloc.dupe(u8, resp.body),
            404 => return error.UnknownGroup,
            405 => return error.UnsupportedOperation,
            409 => return remoteGroupConflictError(resp.body),
            503 => return error.GroupLeaderUnavailable,
            else => return error.UnexpectedHttpStatus,
        }
    }

    fn fetchInternalGroupGet(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        suffix_name: []const u8,
    ) ![]u8 {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return try self.alloc.dupe(u8, resp.body),
            404 => return error.UnknownGroup,
            405 => return error.UnsupportedOperation,
            503 => return error.GroupLeaderUnavailable,
            else => return error.UnexpectedHttpStatus,
        }
    }

    pub fn fetchTables(
        self: *ApiHttpClient,
        base_uri: []const u8,
        prefix: ?[]const u8,
    ) !TablesResponse {
        const path = if (prefix) |pfx|
            try std.fmt.allocPrint(self.alloc, "{s}?prefix={s}", .{ routes.Routes.tables, pfx })
        else
            try self.alloc.dupe(u8, routes.Routes.tables);
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTable(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{ routes.Routes.tables_prefix, table_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn createTable(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{ routes.Routes.tables_prefix, table_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn dropTable(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
    ) !EmptyResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}", .{ routes.Routes.tables_prefix, table_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .DELETE,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 204) return error.UnexpectedHttpStatus;
        return .{};
    }

    pub fn updateTableSchema(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        body: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}/schema", .{ routes.Routes.tables_prefix, table_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .PUT,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTableIndexes(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}/indexes", .{ routes.Routes.tables_prefix, table_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchTableIndex(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        index_name: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}/indexes/{s}", .{ routes.Routes.tables_prefix, table_name, index_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn createTableIndex(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        index_name: []const u8,
        body: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}/indexes/{s}", .{ routes.Routes.tables_prefix, table_name, index_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 201) {
            std.debug.print("createTableIndex unexpected status={d} uri={s} body={s}\n", .{ resp.status, uri, resp.body });
            return error.UnexpectedHttpStatus;
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn deleteTableIndex(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        index_name: []const u8,
    ) !TablesResponse {
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}/indexes/{s}", .{ routes.Routes.tables_prefix, table_name, index_name });
        defer self.alloc.free(path);
        const uri = try self.joinRoute(base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executeRequest(.{
            .method = .DELETE,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 201) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }
};

const EncodedTransitionAction = struct {
    kind: enum {
        prepare_split_source,
        start_split_source,
        bootstrap_split_destination,
        catch_up_split_destination,
        finalize_split_source,
        rollback_split,
        accept_merge_receiver,
        catch_up_merge_receiver,
        finalize_merge,
        rollback_merge,
    },
    transition_id: u64,
    attempt_epoch: u64 = 0,
    source_group_id: ?u64 = null,
    destination_group_id: ?u64 = null,
    donor_group_id: ?u64 = null,
    receiver_group_id: ?u64 = null,
    allow_doc_identity_reassignment: bool = false,
    split_key: ?[]const u8 = null,
    source_range_end: ?[]const u8 = null,
    table_contract: metadata_mod.TransitionTableContract,
};

fn encodeTransitionAction(alloc: std.mem.Allocator, action: metadata_mod.TransitionAction) ![]u8 {
    switch (action) {
        .none => return error.UnsupportedOperation,
        inline else => |op| {
            if (@hasField(@TypeOf(op), "allow_doc_identity_reassignment")) {
                try op.table_contract.validateForMerge(
                    op.allow_doc_identity_reassignment,
                );
            } else {
                try op.table_contract.validateForSplit();
            }
        },
    }
    const encoded: EncodedTransitionAction = switch (action) {
        .none => unreachable,
        .prepare_split_source => |op| .{
            .kind = .prepare_split_source,
            .transition_id = op.transition_id,
            .attempt_epoch = op.attempt_epoch,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
            .split_key = op.split_key,
            .source_range_end = op.source_range_end,
            .table_contract = op.table_contract,
        },
        .start_split_source => |op| .{
            .kind = .start_split_source,
            .transition_id = op.transition_id,
            .attempt_epoch = op.attempt_epoch,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
            .table_contract = op.table_contract,
        },
        .bootstrap_split_destination => |op| .{
            .kind = .bootstrap_split_destination,
            .transition_id = op.transition_id,
            .attempt_epoch = op.attempt_epoch,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
            .table_contract = op.table_contract,
        },
        .catch_up_split_destination => |op| .{
            .kind = .catch_up_split_destination,
            .transition_id = op.transition_id,
            .attempt_epoch = op.attempt_epoch,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
            .table_contract = op.table_contract,
        },
        .finalize_split_source => |op| .{
            .kind = .finalize_split_source,
            .transition_id = op.transition_id,
            .attempt_epoch = op.attempt_epoch,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
            .table_contract = op.table_contract,
        },
        .rollback_split => |op| .{
            .kind = .rollback_split,
            .transition_id = op.transition_id,
            .attempt_epoch = op.attempt_epoch,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
            .table_contract = op.table_contract,
        },
        .accept_merge_receiver => |op| .{
            .kind = .accept_merge_receiver,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
            .allow_doc_identity_reassignment = op.allow_doc_identity_reassignment,
            .table_contract = op.table_contract,
        },
        .catch_up_merge_receiver => |op| .{
            .kind = .catch_up_merge_receiver,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
            .allow_doc_identity_reassignment = op.allow_doc_identity_reassignment,
            .table_contract = op.table_contract,
        },
        .finalize_merge => |op| .{
            .kind = .finalize_merge,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
            .allow_doc_identity_reassignment = op.allow_doc_identity_reassignment,
            .table_contract = op.table_contract,
        },
        .rollback_merge => |op| .{
            .kind = .rollback_merge,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
            .allow_doc_identity_reassignment = op.allow_doc_identity_reassignment,
            .table_contract = op.table_contract,
        },
    };
    return try jsonStringifyAlloc(alloc, encoded);
}

fn jsonStringifyAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn dupeStringSlice(alloc: std.mem.Allocator, items: []const []const u8) ![]const []const u8 {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
        alloc.free(out);
    }
    for (items, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn freeStringSlice(alloc: std.mem.Allocator, items: []const []const u8) void {
    for (items) |item| alloc.free(item);
    if (items.len > 0) alloc.free(items);
}

fn dupeTextIndexEstimates(
    alloc: std.mem.Allocator,
    items: []const db_mod.TextIndexEstimate,
) ![]const db_mod.TextIndexEstimate {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.TextIndexEstimate, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }
    for (items, 0..) |item, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, item.name),
            .doc_count = item.doc_count,
            .chunk_backed = item.chunk_backed,
            .group_chunk_parents = item.group_chunk_parents,
        };
        initialized += 1;
    }
    return out;
}

fn freeTextIndexEstimates(alloc: std.mem.Allocator, items: []const db_mod.TextIndexEstimate) void {
    for (items) |*item| item.deinit(alloc);
    if (items.len > 0) alloc.free(items);
}

fn dupeEmbeddingIndexEstimates(
    alloc: std.mem.Allocator,
    items: []const db_mod.EmbeddingIndexEstimate,
) ![]const db_mod.EmbeddingIndexEstimate {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.EmbeddingIndexEstimate, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }
    for (items, 0..) |item, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, item.name),
            .sparse = item.sparse,
            .doc_count = item.doc_count,
            .dims = item.dims,
            .chunk_backed = item.chunk_backed,
        };
        initialized += 1;
    }
    return out;
}

fn freeEmbeddingIndexEstimates(alloc: std.mem.Allocator, items: []const db_mod.EmbeddingIndexEstimate) void {
    for (items) |*item| item.deinit(alloc);
    if (items.len > 0) alloc.free(items);
}

fn dupeGraphIndexEstimates(
    alloc: std.mem.Allocator,
    items: []const db_mod.GraphIndexEstimate,
) ![]const db_mod.GraphIndexEstimate {
    if (items.len == 0) return &.{};
    const out = try alloc.alloc(db_mod.GraphIndexEstimate, items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }
    for (items, 0..) |item, i| {
        out[i] = .{
            .name = try alloc.dupe(u8, item.name),
            .edge_count = item.edge_count,
            .node_count = item.node_count,
        };
        initialized += 1;
    }
    return out;
}

fn freeGraphIndexEstimates(alloc: std.mem.Allocator, items: []const db_mod.GraphIndexEstimate) void {
    for (items) |*item| item.deinit(alloc);
    if (items.len > 0) alloc.free(items);
}

fn remotePreflightError(body: []const u8) anyerror {
    if (std.mem.eql(u8, body, "InvalidQueryRequest")) return error.InvalidQueryRequest;
    if (std.mem.eql(u8, body, "UnsupportedQueryRequest")) return error.UnsupportedQueryRequest;
    if (std.mem.eql(u8, body, "InvalidArgument")) return error.InvalidArgument;
    if (std.mem.eql(u8, body, "IndexNotFound")) return error.IndexNotFound;
    if (std.mem.eql(u8, body, "TableNotFound")) return error.TableNotFound;
    if (std.mem.eql(u8, body, "UnknownGroup")) return error.UnknownGroup;
    if (std.mem.eql(u8, body, "TopologyChanged")) return error.TopologyChanged;
    if (transactions_api.isTopologyChangedConflictMessage(body)) return error.TopologyChanged;
    if (isDocIdentityNamespaceMismatchConflictMessage(body)) return error.DocIdentityNamespaceMismatch;
    return error.UnexpectedHttpStatus;
}

fn isDocIdentityNamespaceMismatchConflictMessage(body: []const u8) bool {
    return std.mem.eql(u8, body, "doc identity namespace mismatch") or
        std.mem.eql(u8, body, "DocIdentityNamespaceMismatch");
}

fn remoteGroupConflictError(body: []const u8) anyerror {
    if (std.mem.eql(u8, body, "DecisionConflict") or std.mem.eql(u8, body, "decision conflict")) return error.DecisionConflict;
    if (transactions_api.isTopologyChangedConflictMessage(body)) return error.TopologyChanged;
    if (std.mem.eql(u8, body, "TopologyChanged") or std.mem.eql(u8, body, "topology changed")) return error.TopologyChanged;
    if (std.mem.eql(u8, body, "IdentityReadGenerationChanged") or
        std.mem.eql(u8, body, "identity read generation changed")) return error.IdentityReadGenerationChanged;
    if (std.mem.eql(u8, body, "HierarchyCursorStale") or
        std.mem.eql(u8, body, "hierarchy cursor stale")) return error.HierarchyCursorStale;
    if (isDocIdentityNamespaceMismatchConflictMessage(body)) return error.DocIdentityNamespaceMismatch;
    if (std.mem.eql(u8, body, "repair cancelled")) return error.Canceled;
    return error.UnexpectedHttpStatus;
}

fn remoteGraphEdgesError(body: []const u8) anyerror {
    const message = std.mem.trim(u8, body, " \t\r\n");
    if (std.mem.eql(u8, message, "graph explored edges budget exceeded"))
        return error.GraphExploredEdgesBudgetExceeded;
    if (std.mem.eql(u8, message, "graph explored edge bytes budget exceeded"))
        return error.GraphExploredEdgeBytesBudgetExceeded;
    return error.UnexpectedHttpStatus;
}

fn remotePublicBatchError(status: u16, body: []const u8) anyerror {
    const message = std.mem.trim(u8, body, " \t\r\n");
    switch (status) {
        409 => {
            if (std.mem.eql(u8, message, "batch transaction conflicted")) return error.Conflict;
            if (std.mem.eql(u8, message, "write outcome unknown")) return error.RaftBatchWriteOutcomeUnknown;
            if (std.mem.eql(u8, message, "standby is read-only")) return error.HAReadOnlyStandby;
            if (std.mem.eql(u8, message, "promoted standby requires primary open")) {
                return error.HAPromotedStandbyRequiresPrimaryOpen;
            }
            if (std.mem.eql(u8, message, "fenced primary rejects writes")) return error.HAFencedPrimary;
            return remoteGroupConflictError(message);
        },
        503 => {
            if (std.mem.eql(u8, message, "write unavailable")) return error.LeaderUnavailable;
            if (std.mem.eql(u8, message, "doc identity unavailable")) return error.DocIdentityUnavailable;
            if (std.mem.eql(u8, message, "maintenance routes unavailable on query-only runtime")) {
                return error.Unavailable;
            }
            return error.UnexpectedHttpStatus;
        },
        500 => {
            if (std.mem.startsWith(u8, message, "transaction outcome is unknown")) {
                return error.CommitDecisionUnknown;
            }
            return error.UnexpectedHttpStatus;
        },
        else => return error.UnexpectedHttpStatus,
    }
}

test "api http client preserves public batch retry safety classifications" {
    try std.testing.expectEqual(error.Conflict, remotePublicBatchError(409, "batch transaction conflicted"));
    try std.testing.expectEqual(error.RaftBatchWriteOutcomeUnknown, remotePublicBatchError(409, "write outcome unknown"));
    try std.testing.expectEqual(error.CommitDecisionUnknown, remotePublicBatchError(
        500,
        "transaction outcome is unknown; do not retry this stateless batch",
    ));
    try std.testing.expectEqual(error.LeaderUnavailable, remotePublicBatchError(503, "write unavailable"));
    try std.testing.expectEqual(error.HAReadOnlyStandby, remotePublicBatchError(409, "standby is read-only"));
}

fn remoteStorageReadUnavailableError(body: []const u8) anyerror {
    if (std.mem.eql(u8, body, "storage read temporarily unavailable")) {
        return error.StorageReadTemporarilyUnavailable;
    }
    return error.UnexpectedHttpStatus;
}

fn parseIdentityReadGenerationHeader(resp: http_common.HttpResponse) !?u64 {
    const value = resp.header(query_response.QueryResponse.identity_read_generation_header) orelse return null;
    return std.fmt.parseUnsigned(u64, value, 10) catch error.InvalidRemoteResponse;
}

test "api http client preserves remote transaction decision conflicts" {
    try std.testing.expectEqual(error.DecisionConflict, remoteGroupConflictError("decision conflict"));
    try std.testing.expectEqual(error.DecisionConflict, remoteGroupConflictError("DecisionConflict"));
}

test "api http client preserves stale hierarchy cursor conflicts" {
    try std.testing.expectEqual(error.HierarchyCursorStale, remoteGroupConflictError("HierarchyCursorStale"));
    try std.testing.expectEqual(error.HierarchyCursorStale, remoteGroupConflictError("hierarchy cursor stale"));
}

test "api http client preserves remote storage read contention" {
    try std.testing.expectEqual(
        error.StorageReadTemporarilyUnavailable,
        remoteStorageReadUnavailableError("storage read temporarily unavailable"),
    );
    try std.testing.expectEqual(
        error.UnexpectedHttpStatus,
        remoteStorageReadUnavailableError("group leader unavailable"),
    );
}

test "api http client preserves remote graph edge budget exhaustion" {
    try std.testing.expectEqual(
        error.GraphExploredEdgesBudgetExceeded,
        remoteGraphEdgesError("graph explored edges budget exceeded\n"),
    );
    try std.testing.expectEqual(
        error.GraphExploredEdgeBytesBudgetExceeded,
        remoteGraphEdgesError("graph explored edge bytes budget exceeded\n"),
    );
    try std.testing.expectEqual(
        error.UnexpectedHttpStatus,
        remoteGraphEdgesError("invalid graph edge request"),
    );
}

test "api http client preserves storage read contention across group read endpoints" {
    const UnavailableExecutor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            return .{
                .status = 503,
                .body = try alloc.dupe(u8, "storage read temporarily unavailable"),
            };
        }
    };

    const alloc = std.testing.allocator;
    var executor = UnavailableExecutor{};
    var client = ApiHttpClient.init(alloc, executor.executor());
    const base_uri = "http://127.0.0.1:1";
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupLookup(base_uri, 7, "docs", "doc:a", null));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupDocumentArtifactManifest(base_uri, 7, "docs", "doc:a", "chunks"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupDocumentArtifactManifests(base_uri, 7, "docs", "doc:a"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupScan(base_uri, 7, "docs", null));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupQuery(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupQueryPreflight(base_uri, 7, "docs", "{}", 0));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupTextStats(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupAlgebraicPartials(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupGraphExpand(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupGraphHydrate(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupGraphEdges(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, client.fetchGroupVectorWorker(base_uri, 7, "docs", "{}"));
}

test "api http client accepts durable pending batch responses" {
    const PendingExecutor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/tables/docs/batch"));
            return .{
                .status = 202,
                .body = try alloc.dupe(u8, "{\"status\":\"committed_pending\",\"inserted\":1,\"deleted\":0,\"transformed\":0}"),
            };
        }
    };

    var executor = PendingExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    var response = try client.fetchBatch("http://127.0.0.1:1", "docs", "{}");
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), response.status);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"status\":\"committed_pending\"") != null);
}

test "api http client encodes lookup route and query components" {
    const LookupExecutor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expect(std.mem.indexOfScalar(u8, req.uri, 0) == null);
            try std.testing.expect(std.mem.endsWith(
                u8,
                req.uri,
                "/tables/docs%2Ftenant/documents/%01doc%00%00%20asset?fields=title%2Cowner%26admin",
            ));
            return .{
                .status = 200,
                .body = try alloc.dupe(u8, "{}"),
            };
        }
    };

    var executor = LookupExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    var response = try client.fetchLookup(
        "http://127.0.0.1:1",
        "docs/tenant",
        "\x01doc\x00\x00\x20asset",
        "title,owner&admin",
    );
    defer response.deinit(std.testing.allocator);
}

test "api http client preserves retryable group transaction unavailability" {
    const UnavailableExecutor = struct {
        marked_not_proposed: bool = true,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const pre_decision = std.mem.endsWith(u8, req.uri, routes.Routes.txn_begin_suffix) or
                std.mem.endsWith(u8, req.uri, routes.Routes.txn_prepare_suffix);
            if (pre_decision and self.marked_not_proposed) {
                return try http_route_helpers.textResponseWithHeaders(
                    alloc,
                    503,
                    "group leader unavailable",
                    &.{.{
                        .name = txn_contract.pre_decision_outcome_header,
                        .value = txn_contract.pre_decision_not_proposed_v1,
                    }},
                );
            }
            return try http_route_helpers.textResponse(alloc, 503, "group leader unavailable");
        }
    };

    const UntrackedTimeoutExecutor = struct {
        calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
            try std.testing.expectEqual(http_common.RequestDeliveryTracker.State.unknown, tracker.load());
            return error.Timeout;
        }
    };

    var executor = UnavailableExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    const base_uri = "http://127.0.0.1:1";
    try std.testing.expectError(error.GroupLeaderUnavailable, client.fetchGroupTxnBegin(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.GroupLeaderUnavailable, client.fetchGroupTxnPrepare(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.GroupLeaderUnavailable, client.fetchGroupTxnResolve(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.GroupLeaderUnavailable, client.fetchGroupTxnAcknowledge(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.GroupLeaderUnavailable, client.fetchGroupTxnStatus(base_uri, 7, "docs", "{}"));

    executor.marked_not_proposed = false;
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchGroupTxnBegin(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchGroupTxnPrepare(base_uri, 7, "docs", "{}"));

    // Delivery provenance starts before client-local URI construction. An
    // allocation failure here must never be mistaken for an ambiguous send by
    // transaction coordination.
    var begin_failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var begin_setup_client = ApiHttpClient.init(begin_failing.allocator(), executor.executor());
    var begin_delivery: http_common.RequestDeliveryTracker = .{};
    try std.testing.expectError(error.OutOfMemory, begin_setup_client.fetchGroupTxnBeginOutcomeWithDeliveryTracking(
        base_uri,
        7,
        "docs",
        "{}",
        &begin_delivery,
        1_000,
        500,
    ));
    try std.testing.expectEqual(http_common.RequestDeliveryTracker.State.not_sent, begin_delivery.load());

    var prepare_failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var prepare_setup_client = ApiHttpClient.init(prepare_failing.allocator(), executor.executor());
    var prepare_delivery: http_common.RequestDeliveryTracker = .{};
    try std.testing.expectError(error.OutOfMemory, prepare_setup_client.fetchGroupTxnPrepareOutcomeWithDeliveryTracking(
        base_uri,
        7,
        "docs",
        "{}",
        &prepare_delivery,
        1_000,
        500,
    ));
    try std.testing.expectEqual(http_common.RequestDeliveryTracker.State.not_sent, prepare_delivery.load());

    // Crossing into an executor invalidates caller-side `not_sent` proof. An
    // executor that cannot identify its send boundary may leave the state
    // unknown, and transaction routing must fail closed rather than replay.
    var untracked_executor = UntrackedTimeoutExecutor{};
    var untracked_client = ApiHttpClient.init(std.testing.allocator, untracked_executor.executor());
    var untracked_delivery: http_common.RequestDeliveryTracker = .{};
    try std.testing.expectError(error.Timeout, untracked_client.fetchGroupTxnBeginOutcomeWithDeliveryTracking(
        base_uri,
        7,
        "docs",
        "{}",
        &untracked_delivery,
        1_000,
        500,
    ));
    try std.testing.expectEqual(@as(usize, 1), untracked_executor.calls);
    try std.testing.expectEqual(http_common.RequestDeliveryTracker.State.unknown, untracked_delivery.load());

    // Credential construction still occurs before the executor boundary, so
    // a signing allocation failure retains definite no-delivery provenance.
    var signing_failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var signing_delivery: http_common.RequestDeliveryTracker = .{};
    signing_delivery.markNotSent();
    try std.testing.expectError(error.OutOfMemory, internal_service_auth.executeRequest(
        signing_failing.allocator(),
        untracked_executor.executor(),
        .{
            .method = .POST,
            .uri = "http://127.0.0.1:1/internal/v1/groups/7/tables/docs/txn-begin",
            .delivery_tracker = &signing_delivery,
        },
        .{
            .secret = "0123456789abcdef0123456789abcdef",
            .issuer = "cluster-a",
        },
    ));
    try std.testing.expectEqual(@as(usize, 1), untracked_executor.calls);
    try std.testing.expectEqual(http_common.RequestDeliveryTracker.State.not_sent, signing_delivery.load());
}

fn isRetryableMetadataLeaderResponse(resp: http_common.HttpResponse) bool {
    if (resp.status != 503) return false;
    for (resp.headers) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, http_common.metadata_not_leader_header) and
            std.ascii.eqlIgnoreCase(header.value, http_common.metadata_not_leader_value))
        {
            return true;
        }
    }
    return false;
}

fn isTxnPreDecisionNotProposedResponse(resp: http_common.HttpResponse) bool {
    if (resp.status != 404 and resp.status != 503 and resp.status != 504) return false;
    const outcome = resp.header(txn_contract.pre_decision_outcome_header) orelse return false;
    return std.mem.eql(u8, outcome, txn_contract.pre_decision_not_proposed_v1);
}

fn remoteGroupTxnPrepareConflictError(body: []const u8) anyerror {
    if (isDocIdentityNamespaceMismatchConflictMessage(body)) return error.DocIdentityNamespaceMismatch;
    if (transactions_api.isTopologyChangedConflictMessage(body)) return error.TopologyChanged;
    if (std.mem.eql(u8, body, "TopologyChanged")) return error.TopologyChanged;
    if (std.mem.eql(u8, body, "transaction conflict")) return error.IntentConflict;
    return error.UnexpectedHttpStatus;
}

fn remoteGroupTxnResolveConflictError(body: []const u8) anyerror {
    if (isDocIdentityNamespaceMismatchConflictMessage(body)) return error.DocIdentityNamespaceMismatch;
    if (std.mem.eql(u8, body, "topology changed") or std.mem.eql(u8, body, "TopologyChanged")) return error.TopologyChanged;
    if (std.mem.eql(u8, body, "decision conflict")) return error.DecisionConflict;
    return error.UnexpectedHttpStatus;
}

pub fn expectGroupArtifactRepairRunMapsCancelUnavailableForTest() !void {
    const RepairExecutor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.POST, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/internal/v1/groups/7/tables/docs/repair/run"));
            try std.testing.expectEqualStrings("application/json", req.content_type.?);
            try std.testing.expectEqualStrings("{\"target\":\"index\"}", req.body);
            return .{
                .status = 503,
                .body = try alloc.dupe(u8, "repair cancel unavailable"),
            };
        }
    };

    var executor = RepairExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expectError(
        error.RepairCancelUnavailable,
        client.fetchGroupArtifactRepairRun("http://127.0.0.1:1", 7, "docs", "{\"target\":\"index\"}"),
    );
}

test "api http client maps remote repair cancel unavailable" {
    try expectGroupArtifactRepairRunMapsCancelUnavailableForTest();
}

test "api http client bounds transition control RPCs" {
    const TimeoutExecutor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            try std.testing.expectEqual(transition_control_rpc_timeout_ms, req.timeout_ms.?);
            try std.testing.expect(std.mem.endsWith(
                u8,
                req.uri,
                "/internal/v1/groups/7/shard-ops/observe-split",
            ));
            return error.Timeout;
        }
    };

    var executor = TimeoutExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expectError(error.Timeout, client.fetchGroupShardObserveSplit(
        "http://127.0.0.1:1",
        7,
        .{
            .transition_id = 77,
            .attempt_epoch = 1,
            .source_group_id = 7,
            .destination_group_id = 8,
        },
    ));
}

test "api http client forwards internal query controls and maps remote timeout" {
    const TimeoutExecutor = struct {
        calls: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@as(u32, 37), req.timeout_ms.?);
            switch (req.method) {
                .GET => {
                    if (std.mem.indexOf(u8, req.uri, "%01doc%00%00%20asset") != null) {
                        try std.testing.expect(std.mem.indexOfScalar(u8, req.uri, 0) == null);
                        try std.testing.expect(std.mem.indexOf(u8, req.uri, "fields=title%2Cowner%26admin&read_consistency=stale") != null);
                    } else {
                        try std.testing.expect(std.mem.indexOf(u8, req.uri, "/documents/doc%3Aa?") != null);
                        try std.testing.expect(std.mem.indexOf(u8, req.uri, "fields=title&read_consistency=stale") != null);
                    }
                    try std.testing.expect(req.cancellation != null);
                },
                .POST => if (std.mem.indexOf(u8, req.uri, "/join-") == null) {
                    try std.testing.expect(req.cancellation != null);
                },
                else => return error.TestUnexpectedMethod,
            }
            return .{
                // The operation layer reports server-side deadlines as 504;
                // client-side cancellation remains the existing 408 shape.
                .status = if (req.method == .GET) 504 else 408,
                .body = try alloc.dupe(u8, "query timeout"),
            };
        }
    };

    var executor = TimeoutExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    const base_uri = "http://127.0.0.1:1";
    var cancellation = http_common.RequestCancellation{};
    try std.testing.expectError(error.Timeout, client.fetchGroupLookupWithControl(base_uri, 7, "docs", "doc:a", "title", "stale", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupLookupWithControl(base_uri, 7, "docs", "\x01doc\x00\x00\x20asset", "title,owner&admin", "stale", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupQueryWithControl(base_uri, 7, "docs", "{}", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupQueryPreflightWithControl(base_uri, 7, "docs", "{}", 0, 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupTextStatsWithControl(base_uri, 7, "docs", "{}", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupVectorWorkerWithControl(base_uri, 7, "docs", "{}", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupAlgebraicPartialsWithControl(base_uri, 7, "docs", "{}", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupJoinPartitionWithTimeout(base_uri, 7, "docs", "{}", 37));
    try std.testing.expectError(error.Timeout, client.fetchGroupJoinRowsWithTimeout(base_uri, 7, "docs", "{}", 37));
    try std.testing.expectError(error.Timeout, client.fetchGroupJoinUnmatchedWithTimeout(base_uri, 7, "docs", "{}", 37));
    try std.testing.expectError(error.Timeout, client.fetchGroupJoinFinalizeWithTimeout(base_uri, 7, "docs", "{}", 37));
    try std.testing.expectError(error.Timeout, client.fetchGroupGraphExpandWithControl(base_uri, 7, "docs", "{}", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupGraphHydrateWithControl(base_uri, 7, "docs", "{}", 37, &cancellation));
    try std.testing.expectError(error.Timeout, client.fetchGroupGraphEdgesWithControl(base_uri, 7, "docs", "{}", 37, &cancellation));
    try std.testing.expectEqual(@as(usize, 14), executor.calls);
}

test "api http client encodes table name for repair cancel callback" {
    const CancelExecutor = struct {
        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            try std.testing.expectEqual(http_common.Method.GET, req.method);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, "/internal/v1/tables/docs%20table%2Ftenant/repair/jobs/42/attempts/3/cancel-state"));
            return .{
                .status = 200,
                .body = try alloc.dupe(u8, "{\"cancel_requested\":false}"),
            };
        }
    };

    var executor = CancelExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expect(!try client.fetchTableRepairCancelRequested("http://127.0.0.1:1", "docs table/tenant", 42, 3));
}

test "api http client preserves group doc identity conflicts" {
    const ConflictExecutor = struct {
        status: u16,
        body: []const u8,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, _: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = self.status,
                .body = try alloc.dupe(u8, self.body),
            };
        }
    };

    const alloc = std.testing.allocator;
    var conflict_executor = ConflictExecutor{
        .status = 409,
        .body = "doc identity namespace mismatch",
    };
    var client = ApiHttpClient.init(alloc, conflict_executor.executor());
    const base_uri = "http://127.0.0.1:1";

    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupLookup(base_uri, 7, "docs", "a", null));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupQuery(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupQueryPreflight(base_uri, 7, "docs", "{}", 0));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupVectorWorker(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupJoinRows(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupBatch(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupTxnPrepare(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupTxnResolve(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, client.fetchGroupTxnStatus(base_uri, 7, "docs", "{}"));

    conflict_executor.body = "topology changed";
    try std.testing.expectError(error.TopologyChanged, client.fetchGroupVectorWorker(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.TopologyChanged, client.fetchGroupBatch(base_uri, 7, "docs", "{}"));

    conflict_executor.body = "identity read generation changed";
    try std.testing.expectError(error.IdentityReadGenerationChanged, client.fetchGroupQuery(base_uri, 7, "docs", "{}"));
    try std.testing.expectError(error.IdentityReadGenerationChanged, client.fetchGroupGraphExpand(base_uri, 7, "docs", "{}"));

    conflict_executor.status = 503;
    conflict_executor.body = "write unavailable";
    try std.testing.expectError(error.LeaderUnavailable, client.fetchGroupBatch(base_uri, 7, "docs", "{}"));

    conflict_executor.status = 409;
    conflict_executor.body = "write outcome unknown";
    try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, client.fetchGroupBatch(base_uri, 7, "docs", "{}"));
}

test "api http client forwards bounded raft batch routing context without allocation" {
    const ForwardingExecutor = struct {
        response_body_address: usize = 0,
        saw_service_token: bool = false,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(?u32, 500), req.timeout_ms);
            try std.testing.expect(req.cancellation != null);
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.routed_batch_suffix));
            const forwarding = (try internal_batch_forwarding.parse(req)).?;
            try std.testing.expectEqual(@as(u32, 425), forwarding.remaining_ms);
            try std.testing.expectEqual(@as(u8, 1), forwarding.forwards_remaining);
            try std.testing.expect(!forwarding.campaign_allowed);
            for (req.headers) |header| {
                if (!std.ascii.eqlIgnoreCase(header.name, "X-Antfly-Trusted-Principal")) continue;
                try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, header.value, "."));
                self.saw_service_token = true;
            }
            const response_body = try alloc.dupe(u8, "{}");
            self.response_body_address = @intFromPtr(response_body.ptr);
            return .{ .status = 201, .body = response_body };
        }
    };

    var executor = ForwardingExecutor{};
    var cancellation = http_common.RequestCancellation{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    _ = client.withInternalServiceAuth("cluster-secret", "cluster-a");
    var response = try client.fetchGroupBatchWithForwarding(
        "http://127.0.0.1:1",
        7,
        "docs",
        "{}",
        500,
        .{ .remaining_ms = 425, .forwards_remaining = 1, .campaign_allowed = false },
        &cancellation,
    );
    try std.testing.expectEqual(executor.response_body_address, @intFromPtr(response.body.ptr));
    try std.testing.expect(executor.saw_service_token);
    response.deinit(std.testing.allocator);
}

test "api http client authenticates only the internal API namespace" {
    const CaptureExecutor = struct {
        expected_internal: bool = false,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var service_headers: usize = 0;
            for (req.headers) |header| {
                if (!std.ascii.eqlIgnoreCase(header.name, internal_service_auth.header_name)) continue;
                service_headers += 1;
                try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, header.value, "."));
            }
            try std.testing.expectEqual(@as(usize, if (self.expected_internal) 1 else 0), service_headers);
            return .{ .status = 200, .body = try alloc.dupe(u8, "{}") };
        }
    };

    var capture = CaptureExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, capture.executor());
    _ = client.withInternalServiceAuth("cluster-secret", "cluster-a");

    capture.expected_internal = true;
    var exact = try client.executeRequest(.{ .method = .GET, .uri = "http://node:8080/internal/v1?probe=1" });
    exact.deinit(std.testing.allocator);
    const spoofed_headers = [_]http_common.RequestHeader{
        .{ .name = "x-antfly-trusted-principal", .value = "caller-controlled" },
    };
    var nested = try client.executeRequest(.{
        .method = .POST,
        .uri = "/internal/v1/groups/7/txn/prepare",
        .headers = &spoofed_headers,
    });
    nested.deinit(std.testing.allocator);

    capture.expected_internal = false;
    var public = try client.executeRequest(.{ .method = .GET, .uri = "http://node:8080/status" });
    public.deinit(std.testing.allocator);
    var deceptive_host = try client.executeRequest(.{ .method = .GET, .uri = "http://internal.example/internal/v10/groups" });
    deceptive_host.deinit(std.testing.allocator);
    var deceptive_query = try client.executeRequest(.{ .method = .GET, .uri = "http://node:8080/status?next=/internal/v1/groups" });
    deceptive_query.deinit(std.testing.allocator);
}

test "api http client preserves committed visibility outcomes for forwarded raft batches" {
    const OutcomeExecutor = struct {
        body: []const u8,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.routed_batch_suffix));
            return .{ .status = 202, .body = try alloc.dupe(u8, self.body) };
        }
    };

    var executor = OutcomeExecutor{ .body = "committed_visibility_pending" };
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    const forwarding: internal_batch_forwarding.Context = .{
        .remaining_ms = 425,
        .forwards_remaining = 1,
        .campaign_allowed = false,
    };
    try std.testing.expectError(error.EnrichmentRetryInProgress, client.fetchGroupBatchWithForwarding(
        "http://127.0.0.1:1",
        7,
        "docs",
        "{}",
        500,
        forwarding,
        null,
    ));
    executor.body = "committed_repair_required";
    try std.testing.expectError(error.EnrichmentWorkerFailed, client.fetchGroupBatchWithForwarding(
        "http://127.0.0.1:1",
        7,
        "docs",
        "{}",
        500,
        forwarding,
        null,
    ));
}

test "api http client rejects unsupported routed batch protocol without legacy replay" {
    const UnsupportedExecutor = struct {
        attempts: usize = 0,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.attempts += 1;
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.routed_batch_suffix));
            try std.testing.expect((try internal_batch_forwarding.parse(req)) != null);
            return .{ .status = 404, .body = try alloc.dupe(u8, "not found") };
        }
    };

    var executor = UnsupportedExecutor{};
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expectError(error.RaftBatchForwardingUnsupported, client.fetchGroupBatchWithForwarding(
        "http://127.0.0.1:1",
        7,
        "docs",
        "{}",
        500,
        .{ .remaining_ms = 425, .forwards_remaining = 1, .campaign_allowed = false },
        null,
    ));
    try std.testing.expectEqual(@as(usize, 1), executor.attempts);
}

test "api http client requires explicit not-proposed marker and tracks delivery phase" {
    const OutcomeExecutor = struct {
        const Mode = enum {
            unmarked_unavailable,
            marked_not_proposed,
            failure_before_send,
            failure_after_send,
            refused_after_send,
            failure_unknown,
        };

        mode: Mode,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(std.mem.endsWith(u8, req.uri, routes.Routes.routed_batch_suffix));
            const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
            return switch (self.mode) {
                .unmarked_unavailable => try http_route_helpers.textResponse(alloc, 503, "proxy unavailable"),
                .marked_not_proposed => try http_route_helpers.textResponseWithHeaders(
                    alloc,
                    503,
                    "group leader unavailable",
                    &.{.{
                        .name = internal_batch_forwarding.outcome_header,
                        .value = internal_batch_forwarding.outcome_not_proposed_v1,
                    }},
                ),
                .failure_before_send => {
                    tracker.markNotSent();
                    return error.OutOfMemory;
                },
                .failure_after_send => {
                    tracker.markMayHaveBeenSent();
                    return error.OutOfMemory;
                },
                .refused_after_send => {
                    tracker.markMayHaveBeenSent();
                    return error.ConnectionRefused;
                },
                .failure_unknown => return error.OutOfMemory,
            };
        }

        fn fetch(client: *ApiHttpClient) !BatchResponse {
            return client.fetchGroupBatchWithForwarding(
                "http://127.0.0.1:1",
                7,
                "docs",
                "{}",
                500,
                .{ .remaining_ms = 425, .forwards_remaining = 1, .campaign_allowed = false },
                null,
            );
        }
    };

    var executor = OutcomeExecutor{ .mode = .unmarked_unavailable };
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, OutcomeExecutor.fetch(&client));

    executor.mode = .marked_not_proposed;
    try std.testing.expectError(error.LeaderUnavailable, OutcomeExecutor.fetch(&client));

    executor.mode = .failure_before_send;
    try std.testing.expectError(error.OutOfMemory, OutcomeExecutor.fetch(&client));

    executor.mode = .failure_after_send;
    try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, OutcomeExecutor.fetch(&client));

    executor.mode = .refused_after_send;
    try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, OutcomeExecutor.fetch(&client));

    executor.mode = .failure_unknown;
    try std.testing.expectError(error.RaftBatchWriteOutcomeUnknown, OutcomeExecutor.fetch(&client));
}

test "fenced backup forwarding treats post-send transport failure as ambiguous" {
    const Executor = struct {
        fn iface() http_common.RequestExecutor {
            return .{ .ptr = undefined, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const tracker = req.delivery_tracker orelse return error.TestExpectedDeliveryTracker;
            try std.testing.expectEqual(@as(usize, 7), req.headers.len);
            try std.testing.expectEqualStrings(backup_contract.backup_fence_metadata_group_id_header, req.headers[0].name);
            try std.testing.expectEqualStrings("3", req.headers[0].value);
            try std.testing.expectEqualStrings(backup_contract.backup_fence_metadata_incarnation_header, req.headers[1].name);
            try std.testing.expectEqualStrings("0123456789abcdef0123456789abcdef", req.headers[1].value);
            try std.testing.expectEqualStrings(backup_contract.backup_fence_table_id_header, req.headers[2].name);
            try std.testing.expectEqualStrings("7", req.headers[2].value);
            try std.testing.expectEqualStrings(backup_contract.backup_writer_not_after_header, req.headers[6].name);
            try std.testing.expectEqualStrings("123", req.headers[6].value);
            tracker.markMayHaveBeenSent();
            return error.ConnectionResetByPeer;
        }
    };

    var client = ApiHttpClient.init(std.testing.allocator, Executor.iface());
    try std.testing.expectError(error.BackupOutcomeAmbiguous, client.fetchBackupTableFenced(
        "http://127.0.0.1:7777",
        "docs",
        "{}",
        .{
            .metadata_group_id = 3,
            .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
            .table_id = 7,
            .definition_digest = [_]u8{0x11} ** 32,
            .topology_range_count = 1,
            .topology_digest = [_]u8{0x22} ** 32,
            .writer_not_after_unix_ns = 123,
        },
    ));
}

test "api http client encodes merge doc identity reassignment action flag" {
    const alloc = std.testing.allocator;
    const table_contract: metadata_mod.TransitionTableContract = .{
        .table_id = 7,
        .table_name = "docs",
        .schema_json = "",
        .indexes_json = "{}",
        .source_identity = .{ .shard_id = 70, .range_id = 700 },
        .target_identity = .{ .shard_id = 71, .range_id = 701 },
    };
    const body = try encodeTransitionAction(alloc, .{ .finalize_merge = .{
        .transition_id = 8,
        .donor_group_id = 10,
        .receiver_group_id = 9,
        .allow_doc_identity_reassignment = true,
        .table_contract = table_contract,
    } });
    defer alloc.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"allow_doc_identity_reassignment\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"source_identity\":{\"shard_id\":70,\"range_id\":700}") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"target_identity\":{\"shard_id\":71,\"range_id\":701}") != null);
    try std.testing.expectError(
        error.InvalidTransitionTableContract,
        encodeTransitionAction(alloc, .{ .finalize_merge = .{
            .transition_id = 8,
            .donor_group_id = 10,
            .receiver_group_id = 9,
            .table_contract = table_contract,
        } }),
    );
}

test "api http client round-trips public status and internal capability routes" {
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const http_test_runtime = @import("http_test_runtime.zig");
    const http_server = @import("http_server.zig");
    const metadata_api = @import("../metadata/api.zig");

    const FakeSource = struct {
        fn iface(_: *@This()) http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 99,
                .metrics = .{},
                .projected_stores = 2,
                .excluded_stores = 1,
            };
        }
    };

    var source = FakeSource{};
    const service_secret = "http-client-capability-test-secret";
    var server = http_server.ApiHttpServer.init(std.heap.page_allocator, .{
        .internal_service_secret = service_secret,
    }, source.iface(), null, null);
    defer server.deinit();
    var listener = try http_test_runtime.Runtime.startOwned(std.heap.page_allocator, &server);
    defer listener.deinit();
    const base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(base_uri);
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(std.heap.page_allocator, executor.executor());
    _ = client.withInternalServiceAuth(service_secret, null);
    // Dedicated node auth must not make public endpoints require credentials.
    var status = try client.fetchClusterStatus(base_uri);
    defer status.deinit();
    try std.testing.expectEqual(cluster.ClusterHealth.degraded, status.value.health);
    try std.testing.expectEqual(
        internal_batch_forwarding.raft_batch_protocol_version,
        try client.fetchDataRaftBatchProtocolVersion(base_uri, null, null),
    );
}

test "api http client round-trips shard median key route" {
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const http_test_runtime = @import("http_test_runtime.zig");
    const http_server = @import("http_server.zig");
    const metadata_api = @import("../metadata/api.zig");

    const FakeSource = struct {
        fn iface(_: *@This()) http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{
                .metadata_group_id = 99,
                .metrics = .{},
                .projected_stores = 1,
            };
        }
    };

    const FakeShardDb = struct {
        fn adapter() metadata_mod.ShardDbAdapter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .fetch_median_key = fetchMedianKey,
                    .schema_index_ready = schemaIndexReady,
                },
            };
        }

        fn fetchMedianKey(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
            return switch (group_id) {
                77 => try alloc.dupe(u8, "doc:m"),
                88 => null,
                else => error.UnknownGroup,
            };
        }

        fn schemaIndexReady(_: *anyopaque, _: std.mem.Allocator, _: []const u8, group_id: u64, _: u32, _: u32) !bool {
            return switch (group_id) {
                77, 88 => true,
                else => error.UnknownGroup,
            };
        }
    };

    var source = FakeSource{};
    var server = http_server.ApiHttpServer.init(std.heap.page_allocator, .{
        .shard_db_adapter = FakeShardDb.adapter(),
        .internal_service_secret = "http-client-shard-db-test-secret",
    }, source.iface(), null, null);
    defer server.deinit();
    var listener = try http_test_runtime.Runtime.startOwned(std.heap.page_allocator, &server);
    defer listener.deinit();

    const base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(std.heap.page_allocator, executor.executor());
    _ = client.withInternalServiceAuth("http-client-shard-db-test-secret", null);

    const median_key = (try client.fetchGroupDbMedianKey(base_uri, 77)).?;
    defer std.heap.page_allocator.free(median_key);
    try std.testing.expectEqualStrings("doc:m", median_key);

    try std.testing.expect((try client.fetchGroupDbMedianKey(base_uri, 88)) == null);
    try std.testing.expectError(error.UnknownGroup, client.fetchGroupDbMedianKey(base_uri, 99));
}

test "api http client round-trips public table management routes" {
    const http_server = @import("http_server.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const http_test_runtime = @import("http_test_runtime.zig");
    const metadata_api = @import("../metadata/api.zig");
    const metadata_table_manager = @import("../metadata/table_manager.zig");
    const tables_api = @import("tables.zig");

    const FakeSource = struct {
        created: bool = false,
        created_table: ?@import("../metadata/table_manager.zig").TableRecord = null,
        owns_created_table: bool = false,
        indexes_json: []const u8 = "{\"full_text_index_v0\":{}}",
        range_record: @import("../metadata/table_manager.zig").RangeRecord = .{
            .group_id = 10,
            .table_id = 1,
            .start_key = "",
            .end_key = null,
        },
        empty_tables: [0]@import("../metadata/table_manager.zig").TableRecord = .{},
        empty_ranges: [0]@import("../metadata/table_manager.zig").RangeRecord = .{},
        empty_stores: [0]@import("../metadata/table_manager.zig").StoreRecord = .{},
        empty_placements: [0]@import("../raft/reconciler.zig").PlacementIntent = .{},
        empty_splits: [0]@import("../metadata/transition_state.zig").SplitTransitionRecord = .{},
        empty_merges: [0]@import("../metadata/transition_state.zig").MergeTransitionRecord = .{},

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            if (self.owns_created_table and self.created_table != null) {
                metadata_table_manager.freeTable(alloc, self.created_table.?);
            }
        }

        fn tableSlice(self: *@This()) []@import("../metadata/table_manager.zig").TableRecord {
            return @as([*]@import("../metadata/table_manager.zig").TableRecord, @ptrCast(&self.created_table.?))[0..1];
        }

        fn rangeSlice(self: *@This()) []@import("../metadata/table_manager.zig").RangeRecord {
            return @as([*]@import("../metadata/table_manager.zig").RangeRecord, @ptrCast(&self.range_record))[0..1];
        }

        fn iface(self: *@This()) http_server.StatusSource {
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

        fn status(_: *anyopaque) !metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{}, .projected_stores = 1 };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = if (self.created_table) |_|
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

        fn createTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, req: @import("tables.zig").CreateTableRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.created = true;
            _ = table_name;
            _ = req;
            self.created_table = .{
                .table_id = 1,
                .name = "docs",
                .description = "docs table",
                .schema_json = "{\"kind\":\"demo\"}",
                .indexes_json = self.indexes_json,
                .replication_sources_json = "[]",
                .placement_role = "data",
            };
            self.owns_created_table = false;
        }

        fn dropTable(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.owns_created_table and self.created_table != null) {
                metadata_table_manager.freeTable(alloc, self.created_table.?);
            }
            self.created = false;
            self.created_table = null;
            self.owns_created_table = false;
        }

        fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, schema_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.created_table) |*table| {
                const updated = try tables_api.applySchemaUpdateRecord(alloc, table, schema_json);
                if (self.owns_created_table) metadata_table_manager.freeTable(alloc, table.*);
                table.* = updated;
                self.indexes_json = updated.indexes_json;
                self.owns_created_table = true;
            }
        }

        fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, index_name: []const u8, index_json: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const next = try @import("indexes.zig").addIndexToTableIndexesJson(alloc, self.indexes_json, index_name, index_json);
            if (!std.mem.eql(u8, self.indexes_json, "{\"full_text_index_v0\":{}}")) alloc.free(self.indexes_json);
            self.indexes_json = next;
            if (self.created_table) |*table| table.indexes_json = self.indexes_json;
        }

        fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, _: []const u8, index_name: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const next = (try @import("indexes.zig").removeIndexFromTableIndexesJson(alloc, self.indexes_json, index_name)) orelse return error.IndexNotFound;
            if (!std.mem.eql(u8, self.indexes_json, "{\"full_text_index_v0\":{}}")) alloc.free(self.indexes_json);
            self.indexes_json = next;
            if (self.created_table) |*table| table.indexes_json = self.indexes_json;
        }
    };

    var source = FakeSource{};
    defer source.deinit(std.heap.page_allocator);
    var server = http_server.ApiHttpServer.init(std.heap.page_allocator, .{}, source.iface(), null, null);
    defer server.deinit();
    var listener = try http_test_runtime.Runtime.startOwned(std.heap.page_allocator, &server);
    defer listener.deinit();

    const base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(std.heap.page_allocator, executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.testing.allocator, "docs table");
    defer std.testing.allocator.free(create_body);
    var created = try client.createTable(base_uri, "docs", create_body);
    defer created.deinit(std.heap.page_allocator);
    var parsed_created = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, created.body);
    defer parsed_created.deinit();
    try std.testing.expectEqualStrings("docs", parsed_created.value.name);
    try std.testing.expectEqualStrings("docs table", parsed_created.value.description.?);
    try std.testing.expect(parsed_created.value.indexes.map.get("full_text_index_v0") != null);

    var listed = try client.fetchTables(base_uri, null);
    defer listed.deinit(std.heap.page_allocator);
    var parsed_listed = try parseJsonBody([]metadata_openapi.TableStatus, std.testing.allocator, listed.body);
    defer parsed_listed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_listed.value.len);
    try std.testing.expectEqualStrings("docs", parsed_listed.value[0].name);
    try std.testing.expect(parsed_listed.value[0].indexes.map.get("full_text_index_v0") != null);

    var detail = try client.fetchTable(base_uri, "docs");
    defer detail.deinit(std.heap.page_allocator);
    var parsed_detail = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, detail.body);
    defer parsed_detail.deinit();
    try std.testing.expectEqualStrings("docs", parsed_detail.value.name);
    try std.testing.expect(parsed_detail.value.indexes.map.get("full_text_index_v0") != null);

    const schema_body = try test_contract_helpers.encodeSchemaUpdateRequest(std.testing.allocator);
    defer std.testing.allocator.free(schema_body);
    var updated = try client.updateTableSchema(base_uri, "docs", schema_body);
    defer updated.deinit(std.heap.page_allocator);
    var parsed_updated = try parseJsonBody(metadata_openapi.TableStatus, std.testing.allocator, updated.body);
    defer parsed_updated.deinit();
    try std.testing.expect(parsed_updated.value.schema.?.document_schemas != null);

    var indexes = try client.fetchTableIndexes(base_uri, "docs");
    defer indexes.deinit(std.heap.page_allocator);
    var parsed_indexes = try parseJsonBody([]metadata_openapi.IndexStatus, std.testing.allocator, indexes.body);
    defer parsed_indexes.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_indexes.value.len);
    try std.testing.expectEqualStrings("full_text_index_v0", parsed_indexes.value[0].config.name);
    try std.testing.expectEqual(.full_text, parsed_indexes.value[0].config.type);
    try std.testing.expectEqualStrings("full_text_index_v1", parsed_indexes.value[1].config.name);
    try std.testing.expectEqual(.full_text, parsed_indexes.value[1].config.type);

    var index = try client.fetchTableIndex(base_uri, "docs", "full_text_index_v0");
    defer index.deinit(std.heap.page_allocator);
    var parsed_index = try parseJsonBody(metadata_openapi.IndexStatus, std.testing.allocator, index.body);
    defer parsed_index.deinit();
    try std.testing.expectEqual(.full_text, parsed_index.value.config.type);

    const index_body = try test_contract_helpers.encodeCreateIndexRequest(std.testing.allocator, "embed_idx");
    defer std.testing.allocator.free(index_body);
    var created_index = try client.createTableIndex(base_uri, "docs", "embed_idx", index_body);
    defer created_index.deinit(std.heap.page_allocator);

    var index_after_create = try client.fetchTableIndex(base_uri, "docs", "embed_idx");
    defer index_after_create.deinit(std.heap.page_allocator);
    var parsed_index_after_create = try parseJsonBody(metadata_openapi.IndexStatus, std.testing.allocator, index_after_create.body);
    defer parsed_index_after_create.deinit();
    try std.testing.expectEqual(.embeddings, parsed_index_after_create.value.config.type);

    var dropped_index = try client.deleteTableIndex(base_uri, "docs", "embed_idx");
    defer dropped_index.deinit(std.heap.page_allocator);

    var dropped = try client.dropTable(base_uri, "docs");
    defer dropped.deinit(std.heap.page_allocator);
}

test "api http client round-trips public transaction commit route" {
    const http_server = @import("http_server.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const http_test_runtime = @import("http_test_runtime.zig");
    const metadata_api = @import("../metadata/api.zig");
    const raft_mod = @import("../raft/mod.zig");
    const table_reads = @import("table_reads.zig");
    const table_writes = @import("table_writes.zig");

    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-client-txn";
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
        .timestamp_ns = 11,
    });

    var read_source = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var write_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) http_server.StatusSource {
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
    var server = http_server.ApiHttpServer.init(std.heap.page_allocator, .{}, source.iface(), read_source.source(), write_source.source());
    defer server.deinit();
    var listener = try http_test_runtime.Runtime.startOwned(std.heap.page_allocator, &server);
    defer listener.deinit();

    const base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(std.heap.page_allocator, executor.executor());

    const batch_body = try test_contract_helpers.normalizeBatchRequest(std.testing.allocator, "{\"inserts\":{\"doc:a\":{\"title\":\"beta\"}}}");
    defer std.testing.allocator.free(batch_body);
    const commit_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{.{ .table_name = "docs", .key = "doc:a", .version = "11" }},
        &.{.{ .table_name = "docs", .batch_json = batch_body }},
        null,
    );
    defer std.testing.allocator.free(commit_body);

    var committed = try client.fetchTransactionCommit(base_uri, commit_body);
    defer committed.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_commit = try std.json.parseFromSlice(transactions_api.CommitResponse, std.testing.allocator, committed.body, .{});
    defer parsed_commit.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit.value.status);

    const stale_body = try test_contract_helpers.encodeTransactionCommitRequest(
        std.testing.allocator,
        &.{.{ .table_name = "docs", .key = "doc:a", .version = "11" }},
        &.{.{ .table_name = "docs", .batch_json = batch_body }},
        null,
    );
    defer std.testing.allocator.free(stale_body);

    var aborted = try client.fetchTransactionCommit(base_uri, stale_body);
    defer aborted.deinit(std.heap.page_allocator);
    try std.testing.expectEqual(@as(u16, 409), aborted.status);
    var parsed_abort = try std.json.parseFromSlice(transactions_api.CommitResponse, std.testing.allocator, aborted.body, .{});
    defer parsed_abort.deinit();
    try std.testing.expectEqualStrings("aborted", parsed_abort.value.status);
    const stateless_conflict = parsed_abort.value.conflict.?;
    try std.testing.expectEqualStrings("docs", stateless_conflict.table);
    try std.testing.expectEqualStrings("doc:a", stateless_conflict.key);
    try std.testing.expectEqual(@as(?u64, 11), stateless_conflict.expected_version);
    try std.testing.expect(stateless_conflict.current_version != null);
    try std.testing.expect(stateless_conflict.current_version.? > 11);
    // The read-set preflight detects this conflict before participant prepare.
    try std.testing.expect(stateless_conflict.participant == null);
}

test "api http client round-trips long-lived public transaction session routes" {
    const http_server = @import("http_server.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const http_test_runtime = @import("http_test_runtime.zig");
    const metadata_api = @import("../metadata/api.zig");
    const table_reads = @import("table_reads.zig");
    const table_writes = @import("table_writes.zig");
    const raft_mod = @import("../raft/mod.zig");

    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-http-client-session-txn";
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

    var read_source = table_reads.BoundTableReadSource.init("docs", 1, &db, raft_mod.read_gate.noopReadableLeaseRequester());
    var table_source = table_writes.BoundTableWriteSource.init("docs", &db);

    const FakeSource = struct {
        fn iface(_: *@This()) http_server.StatusSource {
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
    var server = http_server.ApiHttpServer.init(alloc, .{}, source.iface(), read_source.source(), table_source.source());
    defer server.deinit();
    var listener = try http_test_runtime.Runtime.startOwned(alloc, &server);
    defer listener.deinit();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(alloc, executor.executor());

    const begin_body = try test_contract_helpers.encodeTransactionBeginRequest(alloc, "write");
    defer alloc.free(begin_body);
    var begin = try client.fetchTransactionBegin(base_uri, begin_body);
    defer begin.deinit(alloc);
    var parsed_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, begin.body, .{});
    defer parsed_begin.deinit();
    const txn_id_hex = parsed_begin.value.transaction_id;

    const read_stage_body = try test_contract_helpers.encodeTransactionStageReadRequest(alloc, "docs", "doc:a", "7");
    defer alloc.free(read_stage_body);
    var read_stage = try client.fetchTransactionSessionRead(base_uri, txn_id_hex, read_stage_body);
    defer read_stage.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), read_stage.status);
    var parsed_read_stage = try std.json.parseFromSlice(transactions_api.StageReadResponse, alloc, read_stage.body, .{});
    defer parsed_read_stage.deinit();
    try std.testing.expectEqualStrings("staged", parsed_read_stage.value.status);
    try std.testing.expectEqualStrings("docs", parsed_read_stage.value.snapshot.table);
    try std.testing.expectEqualStrings("doc:a", parsed_read_stage.value.snapshot.key);
    try std.testing.expectEqualStrings("7", parsed_read_stage.value.snapshot.version);
    try std.testing.expectEqualStrings("alpha", parsed_read_stage.value.snapshot.document.object.get("title").?.string);

    const write_stage_body = try test_contract_helpers.encodeTransactionStageWriteRequest(alloc, "docs", "doc:a", "{\"title\":\"delta\"}");
    defer alloc.free(write_stage_body);
    var write_stage = try client.fetchTransactionSessionWrite(base_uri, txn_id_hex, write_stage_body);
    defer write_stage.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), write_stage.status);

    var session_info = try client.fetchTransactionSessionInfo(base_uri, txn_id_hex);
    defer session_info.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), session_info.status);
    var parsed_session_info = try std.json.parseFromSlice(transactions_api.SessionDetailsResponse, alloc, session_info.body, .{});
    defer parsed_session_info.deinit();
    try std.testing.expectEqualStrings(txn_id_hex, parsed_session_info.value.transaction_id);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.staged_table_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.staged_read_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.staged_write_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.read_snapshot_count);
    try std.testing.expectEqual(false, parsed_session_info.value.durable);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.tables.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.read_snapshots.len);
    try std.testing.expectEqualStrings("docs", parsed_session_info.value.read_snapshots[0].table);
    try std.testing.expectEqualStrings("doc:a", parsed_session_info.value.read_snapshots[0].key);
    try std.testing.expectEqual(@as(u64, 7), parsed_session_info.value.read_snapshots[0].version);
    try std.testing.expectEqualStrings("docs", parsed_session_info.value.tables[0].table);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.tables[0].staged_read_count);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info.value.tables[0].staged_write_count);
    try std.testing.expectEqual(@as(usize, 0), parsed_session_info.value.savepoint_ids.len);

    var session_list = try client.fetchTransactionSessions(base_uri);
    defer session_list.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), session_list.status);
    var parsed_session_list = try std.json.parseFromSlice(transactions_api.SessionListResponse, alloc, session_list.body, .{});
    defer parsed_session_list.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_session_list.value.sessions.len);

    var savepoint = try client.fetchTransactionSessionSavepoint(base_uri, txn_id_hex);
    defer savepoint.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), savepoint.status);
    var parsed_savepoint = try std.json.parseFromSlice(transactions_api.SavepointStatusResponse, alloc, savepoint.body, .{});
    defer parsed_savepoint.deinit();
    const savepoint_id = parsed_savepoint.value.savepoint_id;

    var session_info_with_savepoint = try client.fetchTransactionSessionInfo(base_uri, txn_id_hex);
    defer session_info_with_savepoint.deinit(alloc);
    var parsed_session_info_with_savepoint = try std.json.parseFromSlice(transactions_api.SessionDetailsResponse, alloc, session_info_with_savepoint.body, .{});
    defer parsed_session_info_with_savepoint.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_with_savepoint.value.read_snapshots.len);
    try std.testing.expectEqual(@as(usize, 1), parsed_session_info_with_savepoint.value.savepoint_ids.len);
    try std.testing.expectEqual(savepoint_id, parsed_session_info_with_savepoint.value.savepoint_ids[0]);

    const delete_stage_committed = try test_contract_helpers.encodeTransactionStageDeleteRequest(alloc, "docs", "doc:a");
    defer alloc.free(delete_stage_committed);
    var delete_stage_committed_resp = try client.fetchTransactionSessionDelete(base_uri, txn_id_hex, delete_stage_committed);
    defer delete_stage_committed_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), delete_stage_committed_resp.status);

    var rollback = try client.fetchTransactionSessionRollback(base_uri, txn_id_hex, savepoint_id);
    defer rollback.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), rollback.status);

    var committed = try client.fetchTransactionSessionCommit(base_uri, txn_id_hex, "");
    defer committed.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var parsed_committed = try std.json.parseFromSlice(transactions_api.SessionCommitResponse, alloc, committed.body, .{});
    defer parsed_committed.deinit();
    try std.testing.expectEqualStrings("committed", parsed_committed.value.status);
    try std.testing.expectEqualStrings(txn_id_hex, parsed_committed.value.transaction_id);

    var commit_again = try client.fetchTransactionSessionCommit(base_uri, txn_id_hex, "");
    defer commit_again.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), commit_again.status);
    var parsed_commit_again = try std.json.parseFromSlice(transactions_api.SessionCommitResponse, alloc, commit_again.body, .{});
    defer parsed_commit_again.deinit();
    try std.testing.expectEqualStrings("committed", parsed_commit_again.value.status);
    try std.testing.expectEqualStrings(txn_id_hex, parsed_commit_again.value.transaction_id);

    var abort_begin = try client.fetchTransactionBegin(base_uri, "{}");
    defer abort_begin.deinit(alloc);
    var parsed_abort_begin = try std.json.parseFromSlice(transactions_api.BeginResponse, alloc, abort_begin.body, .{});
    defer parsed_abort_begin.deinit();
    const abort_txn_id_hex = parsed_abort_begin.value.transaction_id;

    const delete_stage_body = try test_contract_helpers.encodeTransactionStageDeleteRequest(alloc, "docs", "doc:a");
    defer alloc.free(delete_stage_body);
    var delete_stage = try client.fetchTransactionSessionDelete(base_uri, abort_txn_id_hex, delete_stage_body);
    defer delete_stage.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), delete_stage.status);

    var aborted = try client.fetchTransactionAbort(base_uri, abort_txn_id_hex);
    defer aborted.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), aborted.status);

    var cleanup = try client.fetchTransactionSessionCleanup(base_uri, std.math.maxInt(u64));
    defer cleanup.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), cleanup.status);
    var parsed_cleanup = try std.json.parseFromSlice(transactions_api.SessionCleanupResponse, alloc, cleanup.body, .{});
    defer parsed_cleanup.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_cleanup.value.removed);
}

test "api http client maps group txn resolve decision conflicts" {
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const http_test_runtime = @import("http_test_runtime.zig");
    const http_server = @import("http_server.zig");
    const metadata_api = @import("../metadata/api.zig");

    const FakeSource = struct {
        fn iface(_: *@This()) http_server.StatusSource {
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
        fn source(_: *@This()) table_writes_api.TableWriteSource {
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
            _: []const txn_api.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?txn_api.CommitOutcome {
            return error.UnsupportedOperation;
        }

        fn commitTransactionWithId(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: db_mod.types.TxnId,
            _: u64,
            _: []const txn_api.TableCommitRequest,
            _: db_mod.types.SyncLevel,
        ) anyerror!?txn_api.CommitOutcome {
            return error.UnsupportedOperation;
        }

        fn beginGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn prepareGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn resolveGroup(_: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64, _: u64, _: db_mod.types.SyncLevel) anyerror!?void {
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            return error.DecisionConflict;
        }

        fn statusGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) anyerror!?db_mod.types.TxnStatus {
            return error.UnsupportedOperation;
        }
    };

    const alloc = std.testing.allocator;
    var source = FakeSource{};
    var writes = FakeWrites{};
    var server = http_server.ApiHttpServer.init(alloc, .{
        .internal_service_secret = "http-client-txn-test-secret",
    }, source.iface(), null, writes.source());
    defer server.deinit();
    var listener = try http_test_runtime.Runtime.startOwned(alloc, &server);
    defer listener.deinit();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(alloc, executor.executor());
    _ = client.withInternalServiceAuth("http-client-txn-test-secret", null);

    const txn_id = try txn_api.parseTxnIdHex("00112233445566778899aabbccddeeff");
    const body = try txn_api.encodeTxnResolveRequest(alloc, .{
        .txn_id = txn_id,
        .status = .committed,
        .commit_version = 10_001,
    });
    defer alloc.free(body);

    try std.testing.expectError(error.DecisionConflict, client.fetchGroupTxnResolve(base_uri, 7001, "docs", body));
}

test "api http client transports txn resolve cancellation and visibility reason" {
    const ResolveExecutor = struct {
        body: []const u8,

        fn executor(self: *@This()) http_common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: http_common.HttpRequest) anyerror!http_common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(req.cancellation != null);
            return .{ .status = 202, .body = try alloc.dupe(u8, self.body) };
        }
    };

    var executor = ResolveExecutor{ .body = "committed_visibility_pending" };
    var client = ApiHttpClient.init(std.testing.allocator, executor.executor());
    var cancellation = http_common.RequestCancellation{};
    try std.testing.expectError(
        error.CommitVisibilityNotSatisfied,
        client.fetchGroupTxnResolveWithControl("http://127.0.0.1:1", 7, "docs", "{}", &cancellation),
    );
    executor.body = "committed_repair_required";
    try std.testing.expectError(
        error.EnrichmentWorkerFailed,
        client.fetchGroupTxnResolveWithControl("http://127.0.0.1:1", 7, "docs", "{}", &cancellation),
    );
}
