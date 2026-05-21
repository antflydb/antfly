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
const cluster = @import("cluster.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const db_api = @import("../storage/db/db.zig");
const db_mod = @import("../storage/db/mod.zig");
const http_common = @import("../raft/transport/http_common.zig");
const distributed_stats_mod = @import("../search/distributed_stats.zig");
const routes = @import("http_routes.zig");
const raft_routes = @import("../raft/transport/routes.zig");
const txn_api = @import("distributed_txn.zig");
const table_writes_api = @import("table_writes.zig");
const test_contract_helpers = @import("test_contract_helpers.zig");
const transactions_api = @import("transactions.zig");
const metadata_openapi = @import("antfly_metadata_openapi");

fn parseJsonBody(comptime T: type, alloc: std.mem.Allocator, body: []const u8) !std.json.Parsed(T) {
    return try std.json.parseFromSlice(T, alloc, body, .{});
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

pub const QueryResponse = struct {
    content_type: ?[]u8 = null,
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
    body: []u8,

    pub fn deinit(self: *BatchResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
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

pub const EmptyResponse = struct {
    pub fn deinit(_: *EmptyResponse, _: std.mem.Allocator) void {}
};

pub const ApiHttpClient = struct {
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,

    pub fn init(alloc: std.mem.Allocator, executor: http_common.RequestExecutor) ApiHttpClient {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    pub fn fetchClusterStatus(self: *ApiHttpClient, base_uri: []const u8) !std.json.Parsed(cluster.ClusterStatus) {
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, routes.Routes.status);
        defer self.alloc.free(uri);
        var resp = try self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        return try std.json.parseFromSlice(cluster.ClusterStatus, self.alloc, resp.body, .{ .allocate = .alloc_always });
    }

    pub fn fetchLookup(
        self: *ApiHttpClient,
        base_uri: []const u8,
        table_name: []const u8,
        key: []const u8,
        fields: ?[]const u8,
    ) !LookupResponse {
        const path = if (fields) |field_list|
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}?fields={s}", .{
                routes.Routes.tables_prefix,
                table_name,
                routes.Routes.lookup_marker,
                key,
                field_list,
            })
        else
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}", .{
                routes.Routes.tables_prefix,
                table_name,
                routes.Routes.lookup_marker,
                key,
            });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const suffix = if (fields) |field_list|
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}?fields={s}", .{
                routes.Routes.tables_prefix,
                table_name,
                routes.Routes.lookup_marker,
                key,
                field_list,
            })
        else
            try std.fmt.allocPrint(self.alloc, "{s}{s}{s}{s}", .{
                routes.Routes.tables_prefix,
                table_name,
                routes.Routes.lookup_marker,
                key,
            });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{ .method = .GET, .uri = uri });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            409 => return remoteGroupConflictError(resp.body),
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
            routes.Routes.lookup_suffix,
        });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const path = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.backup_suffix,
        });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 201) return error.UnexpectedHttpStatus;
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, routes.Routes.backup);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 200) return error.UnexpectedHttpStatus;
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchClusterRestore(
        self: *ApiHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !TablesResponse {
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, routes.Routes.restore);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
            routes.Routes.lookup_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = if (body != null) "application/json" else null,
            .body = body orelse "",
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            409 => return remoteGroupConflictError(resp.body),
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, routes.Routes.agents_retrieval);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.query_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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

    pub fn fetchGroupQueryPreflight(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        max_work: u32,
    ) !db_mod.RuntimePreflightSummary {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.query_preflight_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
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

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = preflight_body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            400 => return remotePreflightError(resp.body),
            404 => return remotePreflightError(resp.body),
            409 => return remotePreflightError(resp.body),
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_partition_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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

    pub fn fetchGroupJoinRows(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_rows_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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

    pub fn fetchGroupJoinUnmatched(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_unmatched_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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

    pub fn fetchGroupTextStats(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.text_stats_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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

    pub fn fetchGroupAlgebraicPartials(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.algebraic_partials_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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

    pub fn fetchGroupJoinFinalize(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !QueryResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.join_finalize_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.graph_expand_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.graph_hydrate_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.graph_edges_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.vector_worker_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => {},
            404 => return error.UnknownGroup,
            409 => return remoteGroupConflictError(resp.body),
            else => return error.UnexpectedHttpStatus,
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 201) {
            std.debug.print("fetchBatch unexpected status={d} uri={s} body={s}\n", .{ resp.status, uri, resp.body });
            return error.UnexpectedHttpStatus;
        }
        return .{
            .body = try self.alloc.dupe(u8, resp.body),
        };
    }

    pub fn fetchTransactionCommit(
        self: *ApiHttpClient,
        base_uri: []const u8,
        body: []const u8,
    ) !TransactionResponse {
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, routes.Routes.transactions_commit);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, routes.Routes.transactions_begin);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, routes.Routes.transactions);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.batch_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status != 201) {
            if (resp.status == 409) return remoteGroupConflictError(resp.body);
            return error.UnexpectedHttpStatus;
        }
        return .{ .body = try self.alloc.dupe(u8, resp.body) };
    }

    pub fn fetchGroupTxnBegin(
        self: *ApiHttpClient,
        base_uri: []const u8,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !EmptyResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.txn_begin_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return .{},
            404 => return error.UnknownGroup,
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
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            routes.Routes.txn_prepare_suffix,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return .{},
            404 => return error.UnknownGroup,
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
        return try fetchInternalPostEmpty(self, base_uri, group_id, table_name, routes.Routes.txn_resolve_suffix, body);
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
    ) !EmptyResponse {
        const suffix = try std.fmt.allocPrint(self.alloc, "{s}{s}{s}", .{
            routes.Routes.tables_prefix,
            table_name,
            suffix_name,
        });
        defer self.alloc.free(suffix);
        const path = try std.fmt.allocPrint(self.alloc, "{s}{d}{s}", .{ routes.Routes.internal_groups_prefix, group_id, suffix });
        defer self.alloc.free(path);
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return .{},
            404 => return error.UnknownGroup,
            405 => return error.UnsupportedOperation,
            409 => return remoteGroupTxnResolveConflictError(resp.body),
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return try self.alloc.dupe(u8, resp.body),
            404 => return error.UnknownGroup,
            405 => return error.UnsupportedOperation,
            409 => return remoteGroupConflictError(resp.body),
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = uri,
        });
        defer resp.deinit(self.alloc);
        switch (resp.status) {
            200 => return try self.alloc.dupe(u8, resp.body),
            404 => return error.UnknownGroup,
            405 => return error.UnsupportedOperation,
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
        const uri = try raft_routes.Routes.join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        var resp = try self.executor.execute(self.alloc, .{
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
    source_group_id: ?u64 = null,
    destination_group_id: ?u64 = null,
    donor_group_id: ?u64 = null,
    receiver_group_id: ?u64 = null,
    allow_doc_identity_reassignment: bool = false,
    split_key: ?[]const u8 = null,
    source_range_end: ?[]const u8 = null,
};

fn encodeTransitionAction(alloc: std.mem.Allocator, action: metadata_mod.TransitionAction) ![]u8 {
    const encoded: EncodedTransitionAction = switch (action) {
        .none => return error.UnsupportedOperation,
        .prepare_split_source => |op| .{
            .kind = .prepare_split_source,
            .transition_id = op.transition_id,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
            .split_key = op.split_key,
            .source_range_end = op.source_range_end,
        },
        .start_split_source => |op| .{
            .kind = .start_split_source,
            .transition_id = op.transition_id,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
        },
        .bootstrap_split_destination => |op| .{
            .kind = .bootstrap_split_destination,
            .transition_id = op.transition_id,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
        },
        .catch_up_split_destination => |op| .{
            .kind = .catch_up_split_destination,
            .transition_id = op.transition_id,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
        },
        .finalize_split_source => |op| .{
            .kind = .finalize_split_source,
            .transition_id = op.transition_id,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
        },
        .rollback_split => |op| .{
            .kind = .rollback_split,
            .transition_id = op.transition_id,
            .source_group_id = op.source_group_id,
            .destination_group_id = op.destination_group_id,
        },
        .accept_merge_receiver => |op| .{
            .kind = .accept_merge_receiver,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
            .allow_doc_identity_reassignment = op.allow_doc_identity_reassignment,
        },
        .catch_up_merge_receiver => |op| .{
            .kind = .catch_up_merge_receiver,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
            .allow_doc_identity_reassignment = op.allow_doc_identity_reassignment,
        },
        .finalize_merge => |op| .{
            .kind = .finalize_merge,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
            .allow_doc_identity_reassignment = op.allow_doc_identity_reassignment,
        },
        .rollback_merge => |op| .{
            .kind = .rollback_merge,
            .transition_id = op.transition_id,
            .donor_group_id = op.donor_group_id,
            .receiver_group_id = op.receiver_group_id,
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
    if (transactions_api.isTopologyChangedConflictMessage(body)) return error.TopologyChanged;
    if (std.mem.eql(u8, body, "TopologyChanged")) return error.TopologyChanged;
    if (isDocIdentityNamespaceMismatchConflictMessage(body)) return error.DocIdentityNamespaceMismatch;
    return error.UnexpectedHttpStatus;
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
    if (std.mem.eql(u8, body, "decision conflict")) return error.DecisionConflict;
    return error.UnexpectedHttpStatus;
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
}

test "api http client encodes merge doc identity reassignment action flag" {
    const alloc = std.testing.allocator;
    const body = try encodeTransitionAction(alloc, .{ .finalize_merge = .{
        .transition_id = 8,
        .donor_group_id = 10,
        .receiver_group_id = 9,
        .allow_doc_identity_reassignment = true,
    } });
    defer alloc.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"allow_doc_identity_reassignment\":true") != null);
}

test "api http client round-trips public status route" {
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");
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
    var server = http_server.ApiHttpServer.init(std.heap.page_allocator, .{}, source.iface(), null, null);
    defer server.deinit();
    var listener = std_http_listener.StdHttpListener.init(std.heap.page_allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(std.heap.page_allocator, executor.executor());
    var status = try client.fetchClusterStatus(base_uri);
    defer status.deinit();
    try std.testing.expectEqual(cluster.ClusterHealth.degraded, status.value.health);
}

test "api http client round-trips shard median key route" {
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");
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
    }, source.iface(), null, null);
    defer server.deinit();
    var listener = std_http_listener.StdHttpListener.init(std.heap.page_allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.heap.page_allocator);
    defer std.heap.page_allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(std.heap.page_allocator, executor.executor());

    const median_key = (try client.fetchGroupDbMedianKey(base_uri, 77)).?;
    defer std.heap.page_allocator.free(median_key);
    try std.testing.expectEqualStrings("doc:m", median_key);

    try std.testing.expect((try client.fetchGroupDbMedianKey(base_uri, 88)) == null);
    try std.testing.expectError(error.UnknownGroup, client.fetchGroupDbMedianKey(base_uri, 99));
}

test "api http client round-trips public table management routes" {
    const http_server = @import("http_server.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");
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
                .replication_sources_json = "[\"seed\"]",
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
    var listener = std_http_listener.StdHttpListener.init(std.heap.page_allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

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
    try std.testing.expectEqual(@as(usize, 1), parsed_indexes.value.len);
    try std.testing.expectEqual(.full_text, parsed_indexes.value[0].config.type);

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
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");
    const metadata_api = @import("../metadata/api.zig");
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
    var server = http_server.ApiHttpServer.init(std.heap.page_allocator, .{}, source.iface(), null, write_source.source());
    defer server.deinit();
    var listener = std_http_listener.StdHttpListener.init(std.heap.page_allocator, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

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
    try std.testing.expectEqual(@as(?u64, 12), stateless_conflict.current_version);
    const stateless_participant = stateless_conflict.participant.?;
    try std.testing.expectEqual(@as(?u64, null), stateless_participant.group_id);
    try std.testing.expectEqualStrings("prepare", stateless_participant.phase.?);
}

test "api http client round-trips long-lived public transaction session routes" {
    const http_server = @import("http_server.zig");
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");
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
    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

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
    try std.testing.expectEqual(@as(u16, 404), commit_again.status);

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
    try std.testing.expectEqual(@as(usize, 0), parsed_cleanup.value.removed);
}

test "api http client maps group txn resolve decision conflicts" {
    const std_http_executor = @import("../raft/transport/std_http_executor.zig");
    const std_http_listener = @import("../raft/transport/std_http_listener.zig");
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

        fn beginGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn prepareGroup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) anyerror!?void {
            return error.UnsupportedOperation;
        }

        fn resolveGroup(_: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) anyerror!?void {
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
    var server = http_server.ApiHttpServer.init(alloc, .{}, source.iface(), null, writes.source());
    defer server.deinit();
    var listener = std_http_listener.StdHttpListener.init(alloc, .{}, server.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(alloc);
    defer alloc.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(alloc, .{});
    defer executor.deinit();
    var client = ApiHttpClient.init(alloc, executor.executor());

    const txn_id = try txn_api.parseTxnIdHex("00112233445566778899aabbccddeeff");
    const body = try txn_api.encodeTxnResolveRequest(alloc, .{
        .txn_id = txn_id,
        .status = .committed,
        .commit_version = 10_001,
    });
    defer alloc.free(body);

    try std.testing.expectError(error.DecisionConflict, client.fetchGroupTxnResolve(base_uri, 7001, "docs", body));
}
