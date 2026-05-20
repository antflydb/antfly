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
const table_reads = @import("table_reads.zig");
const query_api = @import("query.zig");
const query_contract = @import("query_contract.zig");
const foreign_mod = @import("../foreign/mod.zig");
const foreign_sources_api = @import("foreign_sources.zig");
const docstore_mod = @import("../storage/docstore.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const tables_api = @import("tables.zig");
const platform_time = @import("../platform/time.zig");
const db_mod = @import("../storage/db/mod.zig");
const raft_mod = @import("../raft/mod.zig");
const public_table_http = @import("public_table_http.zig");
const join_model = @import("join_model.zig");
const json_helpers = @import("json_helpers.zig");
const unmatched_right_join_group_chunk_limit: u32 = 128;

// ---------------------------------------------------------------------------
// JoinContext vtable
// ---------------------------------------------------------------------------

pub const JoinContext = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        admin_snapshot: *const fn (*anyopaque) anyerror!?metadata_api.AdminSnapshot,
        free_admin_snapshot: *const fn (*anyopaque, *metadata_api.AdminSnapshot) void,
        get_join_shuffle_lease: ?*const fn (*anyopaque, u64) anyerror!?metadata_table_manager.ShuffleJoinLeaseRecord = null,
        upsert_join_shuffle_lease: ?*const fn (*anyopaque, metadata_table_manager.ShuffleJoinLeaseRecord) anyerror!void = null,
        remove_join_shuffle_lease: ?*const fn (*anyopaque, u64) anyerror!void = null,
        execute_plain_query: *const fn (*anyopaque, std.mem.Allocator, table_reads.TableReadSource, []const u8, []const u8, ?[]const u8) anyerror!query_api.QueryResponse,
        execute_query_dispatch: *const fn (*anyopaque, std.mem.Allocator, table_reads.TableReadSource, []const u8, []const u8, ?[]const u8) anyerror![]u8,
        build_owned_search_request: *const fn (*anyopaque, std.mem.Allocator, []const u8, std.json.Value) anyerror!query_api.OwnedQueryRequest,
        ensure_foreign_registry: *const fn (*anyopaque) anyerror!*const foreign_mod.Registry,
    };

    pub fn adminSnapshot(self: JoinContext) !?metadata_api.AdminSnapshot {
        return try self.vtable.admin_snapshot(self.ptr);
    }

    pub fn freeAdminSnapshot(self: JoinContext, snapshot: *metadata_api.AdminSnapshot) void {
        self.vtable.free_admin_snapshot(self.ptr, snapshot);
    }

    pub fn getJoinShuffleLease(self: JoinContext, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
        const fn_ptr = self.vtable.get_join_shuffle_lease orelse return null;
        return try fn_ptr(self.ptr, job_id);
    }

    pub fn upsertJoinShuffleLease(self: JoinContext, record: metadata_table_manager.ShuffleJoinLeaseRecord) !bool {
        const fn_ptr = self.vtable.upsert_join_shuffle_lease orelse return false;
        try fn_ptr(self.ptr, record);
        return true;
    }

    pub fn removeJoinShuffleLease(self: JoinContext, job_id: u64) !bool {
        const fn_ptr = self.vtable.remove_join_shuffle_lease orelse return false;
        try fn_ptr(self.ptr, job_id);
        return true;
    }

    pub fn executePlainQuery(self: JoinContext, alloc: std.mem.Allocator, source: table_reads.TableReadSource, table_name: []const u8, body: []const u8, row_filter_json: ?[]const u8) !query_api.QueryResponse {
        return try self.vtable.execute_plain_query(self.ptr, alloc, source, table_name, body, row_filter_json);
    }

    pub fn executeQueryDispatch(self: JoinContext, alloc: std.mem.Allocator, source: table_reads.TableReadSource, table_name: []const u8, body: []const u8, row_filter_json: ?[]const u8) ![]u8 {
        return try self.vtable.execute_query_dispatch(self.ptr, alloc, source, table_name, body, row_filter_json);
    }

    pub fn buildOwnedSearchRequest(self: JoinContext, alloc: std.mem.Allocator, table_name: []const u8, query_value: std.json.Value) !query_api.OwnedQueryRequest {
        return try self.vtable.build_owned_search_request(self.ptr, alloc, table_name, query_value);
    }

    pub fn ensureForeignRegistry(self: JoinContext) !*const foreign_mod.Registry {
        return try self.vtable.ensure_foreign_registry(self.ptr);
    }

    pub fn supportsSharedJoinShuffleLease(self: JoinContext) bool {
        return self.vtable.get_join_shuffle_lease != null and
            self.vtable.upsert_join_shuffle_lease != null;
    }
};

// ---------------------------------------------------------------------------
// JoinJobStoreConfig
// ---------------------------------------------------------------------------

pub const JoinJobStoreConfig = struct {
    join_job_store_path: ?[]const u8 = null,
    join_job_lease_ttl_ms: ?u64 = null,
    join_job_retention_ms: ?u64 = null,
};

// ---------------------------------------------------------------------------
// Join types
// ---------------------------------------------------------------------------

pub const OpenedJoinJobStore = struct {
    alloc: std.mem.Allocator,
    path_z: [:0]u8,
    docstore: *docstore_mod.DocStore,

    pub fn open(alloc: std.mem.Allocator, path: []const u8) !OpenedJoinJobStore {
        const path_z = try alloc.dupeZ(u8, path);
        errdefer alloc.free(path_z);
        const docstore = try alloc.create(docstore_mod.DocStore);
        errdefer alloc.destroy(docstore);
        docstore.* = try docstore_mod.DocStore.open(alloc, path_z, .{});
        errdefer docstore.close();
        return .{
            .alloc = alloc,
            .path_z = path_z,
            .docstore = docstore,
        };
    }

    pub fn deinit(self: *OpenedJoinJobStore) void {
        self.docstore.close();
        self.alloc.destroy(self.docstore);
        self.alloc.free(self.path_z);
        self.* = undefined;
    }
};

pub const JoinShuffleJobPhase = enum {
    preparing,
    dispatching,
    finalizing,
    succeeded,
    failed,
};

pub const JoinShuffleExecutionMode = enum {
    transient,
    durable,
};

pub const JoinShuffleJobState = struct {
    owner_group_id: ?u64 = null,
    phase: JoinShuffleJobPhase = .preparing,
    total_partitions: usize = 0,
    completed_partitions: usize = 0,
    next_partition_index: usize = 0,
    worker_retries: usize = 0,
    finalizer_retries: usize = 0,
    coordinator_finalized: bool = false,
    last_updated_at_millis: u64 = 0,
    expires_at_millis: u64 = 0,
    last_error: ?[]u8 = null,
    partial_response: ?[]u8 = null,
    cached_response: ?[]u8 = null,

    pub fn deinit(self: *JoinShuffleJobState, alloc: std.mem.Allocator) void {
        if (self.last_error) |value| alloc.free(value);
        if (self.partial_response) |value| alloc.free(value);
        if (self.cached_response) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const EncodedJoinJobState = struct {
    job_id: ?u64 = null,
    owner_group_id: ?u64 = null,
    phase: []const u8,
    total_partitions: u64,
    completed_partitions: u64,
    next_partition_index: ?u64 = null,
    worker_retries: u64,
    finalizer_retries: u64,
    coordinator_finalized: bool,
    last_updated_at_millis: u64,
    expires_at_millis: u64,
    last_error: ?[]const u8 = null,
    partial_response: ?[]const u8 = null,
    cached_response: ?[]const u8 = null,
};

pub const JoinShuffleResumeState = struct {
    result: JoinPartitionExecutionResult,
    next_partition_index: usize,

    pub fn deinit(self: *JoinShuffleResumeState, alloc: std.mem.Allocator) void {
        self.result.deinit(alloc);
        self.* = undefined;
    }
};

pub const SupportedJoinRequest = struct {
    pub const JoinType = enum {
        inner,
        left,
        right,
    };

    right_table: []u8,
    join_type: JoinType = .inner,
    left_field: []u8,
    right_field: []u8,
    right_filters: ?SupportedJoinFilters = null,
    right_fields: [][]const u8 = &.{},
    strategy_hint: ?[]u8 = null,
    nested_join: ?*SupportedJoinRequest = null,
    operator: metadata_openapi.JoinOperator = .eq,

    pub fn deinit(self: *SupportedJoinRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.right_table);
        alloc.free(self.left_field);
        alloc.free(self.right_field);
        if (self.right_filters) |*filters| filters.deinit(alloc);
        for (self.right_fields) |field| alloc.free(@constCast(field));
        if (self.right_fields.len > 0) alloc.free(self.right_fields);
        if (self.strategy_hint) |hint| alloc.free(hint);
        if (self.nested_join) |nested| {
            nested.deinit(alloc);
            alloc.destroy(nested);
        }
        self.* = undefined;
    }
};

pub const SupportedJoinFilters = struct {
    filter_query: ?std.json.Value = null,
    filter_prefix: ?[]u8 = null,
    limit: ?usize = null,

    pub fn deinit(self: *SupportedJoinFilters, alloc: std.mem.Allocator) void {
        if (self.filter_query) |*query| deinitJsonValue(alloc, query);
        if (self.filter_prefix) |prefix| alloc.free(prefix);
        self.* = undefined;
    }
};

pub fn combineFilterQueryWithRowFilterJson(
    alloc: std.mem.Allocator,
    existing_filter_query: ?std.json.Value,
    row_filter_json: []const u8,
) !std.json.Value {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_filter_json, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try combineFilterQueryValues(alloc, existing_filter_query, parsed.value);
}

pub fn applyRightTableRowFilterJson(
    alloc: std.mem.Allocator,
    join: *SupportedJoinRequest,
    row_filter_json: []const u8,
) !void {
    if (join.right_filters == null) join.right_filters = .{};
    var filters = &join.right_filters.?;
    var combined = try combineFilterQueryWithRowFilterJson(alloc, filters.filter_query, row_filter_json);
    errdefer deinitJsonValue(alloc, &combined);
    if (filters.filter_query) |*existing| deinitJsonValue(alloc, existing);
    filters.filter_query = combined;
}

fn combineFilterQueryValues(
    alloc: std.mem.Allocator,
    existing_filter_query: ?std.json.Value,
    row_filter: std.json.Value,
) !std.json.Value {
    const existing = existing_filter_query orelse return try cloneJsonValue(alloc, row_filter);

    var conjuncts = std.json.Array.init(alloc);
    var conjuncts_owned = true;
    errdefer if (conjuncts_owned) {
        for (conjuncts.items) |*item| deinitJsonValue(alloc, item);
        conjuncts.deinit();
    };
    var existing_clone = try cloneJsonValue(alloc, existing);
    var existing_clone_owned = true;
    errdefer if (existing_clone_owned) deinitJsonValue(alloc, &existing_clone);
    try conjuncts.append(existing_clone);
    existing_clone_owned = false;

    var row_filter_clone = try cloneJsonValue(alloc, row_filter);
    var row_filter_clone_owned = true;
    errdefer if (row_filter_clone_owned) deinitJsonValue(alloc, &row_filter_clone);
    try conjuncts.append(row_filter_clone);
    row_filter_clone_owned = false;

    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer deinitJsonValue(alloc, &root);
    const key = try alloc.dupe(u8, "conjuncts");
    errdefer alloc.free(key);
    try root.object.put(alloc, key, .{ .array = conjuncts });
    conjuncts_owned = false;
    return root;
}

pub const JoinedQueryStats = struct {
    left_rows_scanned: i64 = 0,
    right_rows_scanned: i64 = 0,
    rows_matched: i64 = 0,
    rows_unmatched_left: i64 = 0,
    rows_unmatched_right: i64 = 0,
};

pub const JoinTableStats = struct {
    row_count: u64 = 0,
    size_bytes: u64 = 0,
    shard_count: usize = 0,
    has_stats: bool = false,
};

pub const PlannedJoinExecution = struct {
    strategy: RightJoinQueryResult.StrategyUsed = .broadcast,
    estimated_cost: f64 = 0,
    estimated_rows: u64 = 0,
    estimated_memory_bytes: u64 = 0,
    used_stats: bool = false,
    shuffle_partitions: usize = 0,
    shuffle_candidate: bool = false,
    forced_broadcast_fallback: bool = false,
};

const StatefulJoinStrategyDecision = struct {
    strategy: RightJoinQueryResult.StrategyUsed,
    shuffle_partitions: usize = 0,
    shuffle_candidate: bool = false,
    forced_broadcast_fallback: bool = false,

    fn apply(self: StatefulJoinStrategyDecision, plan: *PlannedJoinExecution) void {
        plan.strategy = self.strategy;
        plan.shuffle_partitions = self.shuffle_partitions;
        plan.shuffle_candidate = self.shuffle_candidate;
        plan.forced_broadcast_fallback = self.forced_broadcast_fallback;
    }
};

pub const RightJoinQueryResult = struct {
    pub const StrategyUsed = enum {
        index_lookup,
        broadcast,
        shuffle,
    };

    parsed: ?std.json.Parsed(std.json.Value) = null,
    hits: []const std.json.Value = &.{},
    owned_hits: []std.json.Value = &.{},
    strategy_used: StrategyUsed = .index_lookup,
    distributed_execution: bool = false,
    groups_queried: usize = 0,

    pub fn deinit(self: *RightJoinQueryResult, alloc: std.mem.Allocator) void {
        if (self.parsed) |*parsed| parsed.deinit();
        if (self.owned_hits.len > 0) {
            for (self.owned_hits) |*hit| deinitJsonValue(alloc, hit);
            alloc.free(self.owned_hits);
        }
        self.* = undefined;
    }
};

pub const JoinPartitionExecutionResult = struct {
    pub const WorkerAttempt = struct {
        partition_index: usize,
        worker_group_id: u64,
        succeeded: bool,
    };

    pub const FinalizerAttempt = struct {
        worker_group_id: u64,
        succeeded: bool,
    };

    hits: []std.json.Value,
    stats: JoinedQueryStats = .{},
    groups_queried: usize = 0,
    execution_mode: JoinShuffleExecutionMode = .transient,
    matched_right_ids: [][]u8 = &.{},
    job_id: ?u64 = null,
    job_phase: ?JoinShuffleJobPhase = null,
    total_partitions: usize = 0,
    completed_partitions: usize = 0,
    expires_at_millis: u64 = 0,
    worker_retries: usize = 0,
    finalizer_retries: usize = 0,
    finalizer_group_id: ?u64 = null,
    coordinator_finalized: bool = false,
    imported_owner_group_id: ?u64 = null,
    imported_partial_state: bool = false,
    imported_cached_result: bool = false,
    worker_attempts: []WorkerAttempt = &.{},
    finalizer_attempts: []FinalizerAttempt = &.{},

    pub fn deinit(self: *JoinPartitionExecutionResult, alloc: std.mem.Allocator) void {
        for (self.hits) |*hit| deinitJsonValue(alloc, hit);
        if (self.hits.len > 0) alloc.free(self.hits);
        for (self.matched_right_ids) |id| alloc.free(id);
        if (self.matched_right_ids.len > 0) alloc.free(self.matched_right_ids);
        if (self.worker_attempts.len > 0) alloc.free(self.worker_attempts);
        if (self.finalizer_attempts.len > 0) alloc.free(self.finalizer_attempts);
        self.* = undefined;
    }
};

const DistributedRightJoinUnmatchedCandidates = struct {
    right_result: RightJoinQueryResult,
    matched_right_ids: std.StringHashMapUnmanaged(void) = .{},

    fn deinit(self: *DistributedRightJoinUnmatchedCandidates, alloc: std.mem.Allocator) void {
        self.right_result.deinit(alloc);
        self.matched_right_ids.deinit(alloc);
        self.* = undefined;
    }
};

const DistributedRightJoinUnmatchedCompletion = struct {
    hits: []std.json.Value,
    groups_queried: usize,
    right_rows_scanned: usize,

    fn deinit(self: *DistributedRightJoinUnmatchedCompletion, alloc: std.mem.Allocator) void {
        for (self.hits) |*item| deinitJsonValue(alloc, item);
        if (self.hits.len > 0) alloc.free(self.hits);
        self.* = undefined;
    }
};

pub const JoinedRightMergeResult = join_model.JoinOwnedShell(JoinedQueryStats);

pub const LoadedRightJoinQuery = struct {
    parsed: std.json.Parsed(std.json.Value),
    hits: []std.json.Value,

    pub fn deinit(self: *LoadedRightJoinQuery) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub const JoinPartitionRequest = struct {
    job_id: ?u64 = null,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    appended_left_field: bool = false,
    partition_index: usize = 0,
    partition_count: usize = 1,
    right_group_ids: []const u64 = &.{},
    parsed: std.json.Parsed(EncodedJoinPartitionRequest),

    pub fn deinit(self: *JoinPartitionRequest, alloc: std.mem.Allocator) void {
        freeSupportedJoinRequest(alloc, &self.join);
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub const JoinRowsRequest = struct {
    job_id: ?u64 = null,
    join: SupportedJoinRequest,
    partition_index: usize = 0,
    partition_count: usize = 1,
    parsed: std.json.Parsed(EncodedJoinRowsRequest),

    pub fn deinit(self: *JoinRowsRequest, alloc: std.mem.Allocator) void {
        freeSupportedJoinRequest(alloc, &self.join);
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub const JoinUnmatchedRequest = struct {
    join: SupportedJoinRequest,
    left_hit_count: usize = 0,
    left_fields: []const []const u8 = &.{},
    appended_left_field: bool = false,
    matched_right_ids: []const []const u8 = &.{},
    parsed: std.json.Parsed(EncodedJoinUnmatchedRequest),

    pub fn deinit(self: *JoinUnmatchedRequest, alloc: std.mem.Allocator) void {
        freeSupportedJoinRequest(alloc, &self.join);
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub const JoinFinalizeRequest = struct {
    job_id: ?u64 = null,
    handoff_owner_group_id: ?u64 = null,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    left_fields: []const std.json.Value = &.{},
    appended_left_field: bool = false,
    shuffle_partitions: usize = 1,
    parsed: std.json.Parsed(EncodedJoinFinalizeRequest),

    pub fn deinit(self: *JoinFinalizeRequest, alloc: std.mem.Allocator) void {
        freeSupportedJoinRequest(alloc, &self.join);
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub const EncodedJoinPartitionRequest = struct {
    job_id: ?u64 = null,
    join: metadata_openapi.JoinClause,
    left_hits: []const std.json.Value,
    appended_left_field: ?bool = null,
    partition_index: ?u64 = null,
    partition_count: ?u64 = null,
    right_group_ids: ?[]const u64 = null,
};

pub const EncodedJoinRowsRequest = struct {
    job_id: ?u64 = null,
    join: metadata_openapi.JoinClause,
    partition_index: ?u64 = null,
    partition_count: ?u64 = null,
};

pub const EncodedJoinUnmatchedRequest = struct {
    join: metadata_openapi.JoinClause,
    left_hit_count: ?u64 = null,
    left_fields: ?[]const []const u8 = null,
    appended_left_field: ?bool = null,
    matched_right_ids: ?[]const []const u8 = null,
};

pub const EncodedJoinFinalizeRequest = struct {
    job_id: ?u64 = null,
    handoff_owner_group_id: ?u64 = null,
    join: metadata_openapi.JoinClause,
    left_hits: []const std.json.Value,
    left_fields: ?[]const std.json.Value = null,
    appended_left_field: ?bool = null,
    shuffle_partitions: ?u64 = null,
};

pub const EncodedJoinRowsResponse = struct {
    hits: []const std.json.Value,
};

pub const EncodedJoinUnmatchedResponse = struct {
    hits: []const std.json.Value,
    right_rows_scanned: u64,
};

pub const EncodedJoinJobStateRequest = struct {
    job_id: u64,
};

pub const EncodedJoinPartitionStats = struct {
    left_rows_scanned: i64,
    right_rows_scanned: i64,
    rows_matched: i64,
    rows_unmatched_left: i64 = 0,
    rows_unmatched_right: i64 = 0,
};

pub const EncodedJoinPartitionWorkerAttempt = struct {
    partition_index: u64,
    worker_group_id: u64,
    succeeded: bool,
};

pub const EncodedJoinPartitionFinalizerAttempt = struct {
    worker_group_id: u64,
    succeeded: bool,
};

pub const EncodedJoinPartitionResponse = struct {
    hits: []const std.json.Value,
    stats: EncodedJoinPartitionStats,
    matched_right_ids: ?[]const []const u8 = null,
    job_id: ?u64 = null,
    job_phase: ?[]const u8 = null,
    total_partitions: ?u64 = null,
    completed_partitions: ?u64 = null,
    expires_at_millis: ?u64 = null,
    worker_retries: ?u64 = null,
    finalizer_retries: ?u64 = null,
    finalizer_group_id: ?u64 = null,
    coordinator_finalized: ?bool = null,
    imported_owner_group_id: ?u64 = null,
    imported_partial_state: ?bool = null,
    imported_cached_result: ?bool = null,
    worker_attempts: ?[]const EncodedJoinPartitionWorkerAttempt = null,
    finalizer_attempts: ?[]const EncodedJoinPartitionFinalizerAttempt = null,
};

pub const ParsedSupportedJoinRequest = struct {
    join: SupportedJoinRequest,
    foreign_sources: foreign_mod.PostgresSourceMap = .{},

    pub fn deinit(self: *ParsedSupportedJoinRequest, alloc: std.mem.Allocator) void {
        freeSupportedJoinRequest(alloc, &self.join);
        self.foreign_sources.deinit(alloc);
        self.* = undefined;
    }
};

pub const JoinedBaseQueryRewrite = struct {
    body: []u8,
    appended_left_field: bool,
};

pub const OwnedRequestedFields = struct {
    values: ?[]const []const u8 = null,
    owned: [][]u8 = &.{},
    appended: bool = false,

    pub fn deinit(self: *OwnedRequestedFields, alloc: std.mem.Allocator) void {
        for (self.owned) |value| alloc.free(value);
        if (self.owned.len > 0) alloc.free(self.owned);
        self.* = undefined;
    }
};

// ---------------------------------------------------------------------------
// JoinJobStore
// ---------------------------------------------------------------------------

pub const JoinJobStore = struct {
    alloc: std.mem.Allocator,
    ctx: ?JoinContext = null,
    cfg: JoinJobStoreConfig,
    opened_join_job_store: ?*OpenedJoinJobStore = null,
    join_jobs_mutex: std.atomic.Mutex = .unlocked,
    join_jobs: std.AutoHashMapUnmanaged(u64, JoinShuffleJobState) = .{},
    next_join_job_id: u64 = 1,

    pub fn init(alloc: std.mem.Allocator, cfg: JoinJobStoreConfig) JoinJobStore {
        return .{
            .alloc = alloc,
            .cfg = cfg,
        };
    }

    pub fn initWithStore(alloc: std.mem.Allocator, cfg: JoinJobStoreConfig) !JoinJobStore {
        var self = init(alloc, cfg);
        if (cfg.join_job_store_path) |path| {
            const store = try alloc.create(OpenedJoinJobStore);
            errdefer alloc.destroy(store);
            store.* = try OpenedJoinJobStore.open(alloc, path);
            self.opened_join_job_store = store;
        }
        return self;
    }

    pub fn deinit(self: *JoinJobStore) void {
        var it = self.join_jobs.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit(self.alloc);
        }
        self.join_jobs.deinit(self.alloc);
        if (self.opened_join_job_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        self.* = undefined;
    }

    pub fn setContext(self: *JoinJobStore, ctx: JoinContext) void {
        self.ctx = ctx;
    }

    // -- timing helpers --

    fn joinJobLeaseTtlMillis(self: *const JoinJobStore) u64 {
        return self.cfg.join_job_lease_ttl_ms orelse 30_000;
    }

    fn joinJobRetentionMillis(self: *const JoinJobStore) u64 {
        return self.cfg.join_job_retention_ms orelse 300_000;
    }

    pub fn joinJobExpiryForPhase(self: *const JoinJobStore, phase: JoinShuffleJobPhase, now_ms: u64) u64 {
        return now_ms + switch (phase) {
            .preparing, .dispatching, .finalizing => self.joinJobLeaseTtlMillis(),
            .succeeded, .failed => self.joinJobRetentionMillis(),
        };
    }

    // -- id generation --

    pub fn nextJoinJobId(self: *JoinJobStore) u64 {
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const job_id = self.next_join_job_id;
        self.next_join_job_id += 1;
        return job_id;
    }

    pub fn stableDistributedJoinJobId(
        self: *JoinJobStore,
        alloc: std.mem.Allocator,
        join: SupportedJoinRequest,
        left_hits: []const std.json.Value,
        appended_left_field: bool,
        shuffle_partitions: usize,
    ) !u64 {
        _ = self;
        var hasher = std.hash.Wyhash.init(0);
        var join_value = try encodeSupportedJoinClauseValue(alloc, join);
        defer deinitJsonValue(alloc, &join_value);
        const join_json = try stringifyJsonValueAlloc(alloc, join_value);
        defer alloc.free(join_json);
        hasher.update(join_json);
        hasher.update(if (appended_left_field) "1" else "0");
        var partition_buf: [32]u8 = undefined;
        const partition_text = try std.fmt.bufPrint(&partition_buf, "{d}", .{shuffle_partitions});
        hasher.update(partition_text);
        for (left_hits, 0..) |hit, i| {
            if (hit == .object) {
                if (hit.object.get("_id")) |id_value| {
                    if (id_value == .string) {
                        hasher.update(id_value.string);
                        continue;
                    }
                }
            }
            var index_buf: [32]u8 = undefined;
            const index_text = try std.fmt.bufPrint(&index_buf, "#{d}", .{i});
            hasher.update(index_text);
        }
        const job_id = hasher.final();
        return if (job_id == 0) 1 else job_id;
    }

    // -- lease helpers --

    pub fn syncSharedJoinShuffleLease(self: *JoinJobStore, job_id: u64, owner_group_id: ?u64, phase: JoinShuffleJobPhase) void {
        const owner = owner_group_id orelse return;
        const ctx = self.ctx orelse return;
        _ = ctx.upsertJoinShuffleLease(.{
            .job_id = job_id,
            .owner_group_id = owner,
            .expires_at_ms = self.joinJobExpiryForPhase(phase, joinJobNowMillis()),
        }) catch |err| {
            std.log.warn("distributed join lease sync failed job_id={d} owner_group_id={d} err={}", .{ job_id, owner, err });
            return;
        };
    }

    pub fn clearSharedJoinShuffleLease(self: *JoinJobStore, job_id: u64) void {
        const ctx = self.ctx orelse return;
        _ = ctx.removeJoinShuffleLease(job_id) catch |err| {
            std.log.warn("distributed join lease removal failed job_id={d} err={}", .{ job_id, err });
            return;
        };
    }

    pub fn sharedJoinShuffleFinalizerStartIndex(self: *JoinJobStore, job_id: u64, worker_group_ids: []const u64) usize {
        const deterministic_index = preferredFinalizerStartIndex(job_id, worker_group_ids);
        const ctx = self.ctx orelse return deterministic_index;
        const now_ms = joinJobNowMillis();
        const projected = ctx.getJoinShuffleLease(job_id) catch |err| {
            std.log.warn("distributed join lease lookup failed job_id={d} err={}", .{ job_id, err });
            self.syncSharedJoinShuffleLease(job_id, worker_group_ids[deterministic_index], .finalizing);
            return deterministic_index;
        };
        if (projected) |lease| {
            if (lease.expires_at_ms > now_ms) {
                if (indexOfWorkerGroup(worker_group_ids, lease.owner_group_id)) |index| {
                    self.syncSharedJoinShuffleLease(job_id, lease.owner_group_id, .finalizing);
                    return index;
                }
            }
        }
        self.syncSharedJoinShuffleLease(job_id, worker_group_ids[deterministic_index], .finalizing);
        return deterministic_index;
    }

    pub fn shouldUseDurableDistributedJoin(
        self: *const JoinJobStore,
        plan: PlannedJoinExecution,
        left_hit_count: usize,
        worker_group_count: usize,
    ) bool {
        if (plan.strategy != .shuffle) return false;
        if (worker_group_count <= 1) return false;
        if (self.ctx == null or !self.ctx.?.supportsSharedJoinShuffleLease()) return false;
        if (self.opened_join_job_store == null) return false;
        return left_hit_count >= 64 or plan.shuffle_partitions > 2;
    }

    // -- persistence --

    fn persistJoinJobState(self: *JoinJobStore, job_id: u64, state: JoinShuffleJobState) !void {
        const opened = self.opened_join_job_store orelse return;
        const key = try joinJobKey(self.alloc, job_id);
        defer self.alloc.free(key);
        const encoded = try encodeJoinJobState(self.alloc, job_id, state);
        defer self.alloc.free(encoded);
        try opened.docstore.put(key, encoded);
    }

    fn loadPersistedJoinJobState(self: *JoinJobStore, alloc: std.mem.Allocator, job_id: u64) !?JoinShuffleJobState {
        const opened = self.opened_join_job_store orelse return null;
        const key = try joinJobKey(alloc, job_id);
        defer alloc.free(key);
        const body = opened.docstore.get(alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer alloc.free(body);
        return try parseJoinJobState(alloc, body);
    }

    fn deletePersistedJoinJobState(self: *JoinJobStore, alloc: std.mem.Allocator, job_id: u64) void {
        const opened = self.opened_join_job_store orelse return;
        const key = joinJobKey(alloc, job_id) catch return;
        defer alloc.free(key);
        opened.docstore.delete(key) catch {};
    }

    // -- cleanup --

    pub fn cleanupExpiredJoinJobs(self: *JoinJobStore) void {
        const now_ms = joinJobNowMillis();
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        var expired = std.ArrayListUnmanaged(u64).empty;
        defer expired.deinit(self.alloc);
        var it = self.join_jobs.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.expires_at_millis == 0 or entry.value_ptr.expires_at_millis > now_ms) continue;
            expired.append(self.alloc, entry.key_ptr.*) catch continue;
        }
        for (expired.items) |job_id| {
            if (self.join_jobs.fetchRemove(job_id)) |removed| {
                var state = removed.value;
                state.deinit(self.alloc);
                self.deletePersistedJoinJobState(self.alloc, job_id);
                self.clearSharedJoinShuffleLease(job_id);
            }
        }
    }

    // -- recording --

    pub fn recordJoinJobStart(self: *JoinJobStore, job_id: u64, owner_group_id: ?u64, total_partitions: usize) !void {
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const entry = try self.join_jobs.getOrPut(self.alloc, job_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = .{};
        }
        if (entry.value_ptr.last_error) |value| {
            self.alloc.free(value);
            entry.value_ptr.last_error = null;
        }
        if (entry.value_ptr.partial_response) |value| {
            self.alloc.free(value);
            entry.value_ptr.partial_response = null;
        }
        if (entry.value_ptr.cached_response) |value| {
            self.alloc.free(value);
            entry.value_ptr.cached_response = null;
        }
        entry.value_ptr.owner_group_id = owner_group_id;
        entry.value_ptr.phase = .dispatching;
        entry.value_ptr.total_partitions = total_partitions;
        entry.value_ptr.completed_partitions = 0;
        entry.value_ptr.next_partition_index = 0;
        entry.value_ptr.worker_retries = 0;
        entry.value_ptr.finalizer_retries = 0;
        entry.value_ptr.coordinator_finalized = false;
        entry.value_ptr.last_updated_at_millis = joinJobNowMillis();
        entry.value_ptr.expires_at_millis = self.joinJobExpiryForPhase(.dispatching, entry.value_ptr.last_updated_at_millis);
        try self.persistJoinJobState(job_id, entry.value_ptr.*);
    }

    pub fn recordJoinJobProgress(
        self: *JoinJobStore,
        job_id: u64,
        next_partition_index: usize,
        partial_result: JoinPartitionExecutionResult,
    ) !void {
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const state = self.join_jobs.getPtr(job_id) orelse return;
        const encoded = try self.encodeJoinPartitionResponse(self.alloc, partial_result);
        errdefer self.alloc.free(encoded);
        if (state.partial_response) |value| self.alloc.free(value);
        state.partial_response = encoded;
        state.completed_partitions = partial_result.completed_partitions;
        state.next_partition_index = next_partition_index;
        state.worker_retries = partial_result.worker_retries;
        state.phase = .finalizing;
        state.last_updated_at_millis = joinJobNowMillis();
        state.expires_at_millis = self.joinJobExpiryForPhase(.finalizing, state.last_updated_at_millis);
        try self.persistJoinJobState(job_id, state.*);
    }

    pub fn recordJoinJobSucceeded(
        self: *JoinJobStore,
        job_id: u64,
        finalizer_group_id: ?u64,
        finalizer_retries: usize,
        coordinator_finalized: bool,
        encoded_response: []const u8,
    ) !void {
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const state = self.join_jobs.getPtr(job_id) orelse return;
        if (state.cached_response) |value| self.alloc.free(value);
        if (state.partial_response) |value| {
            self.alloc.free(value);
            state.partial_response = null;
        }
        if (state.last_error) |value| {
            self.alloc.free(value);
            state.last_error = null;
        }
        state.owner_group_id = finalizer_group_id;
        state.phase = .succeeded;
        state.completed_partitions = state.total_partitions;
        state.next_partition_index = state.total_partitions;
        state.finalizer_retries = finalizer_retries;
        state.coordinator_finalized = coordinator_finalized;
        state.cached_response = try self.alloc.dupe(u8, encoded_response);
        state.last_updated_at_millis = joinJobNowMillis();
        state.expires_at_millis = self.joinJobExpiryForPhase(.succeeded, state.last_updated_at_millis);
        try self.persistJoinJobState(job_id, state.*);
    }

    pub fn recordJoinJobFailed(self: *JoinJobStore, job_id: u64, err: anyerror) !void {
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const state = self.join_jobs.getPtr(job_id) orelse return;
        if (state.last_error) |value| self.alloc.free(value);
        if (state.partial_response) |value| {
            self.alloc.free(value);
            state.partial_response = null;
        }
        state.phase = .failed;
        state.last_error = try std.fmt.allocPrint(self.alloc, "{s}", .{@errorName(err)});
        state.last_updated_at_millis = joinJobNowMillis();
        state.expires_at_millis = self.joinJobExpiryForPhase(.failed, state.last_updated_at_millis);
        try self.persistJoinJobState(job_id, state.*);
        self.clearSharedJoinShuffleLease(job_id);
    }

    // -- loading --

    pub fn loadJoinJobCachedResult(self: *JoinJobStore, alloc: std.mem.Allocator, job_id: u64) !?JoinPartitionExecutionResult {
        self.cleanupExpiredJoinJobs();
        const now_ms = joinJobNowMillis();
        lockAtomic(&self.join_jobs_mutex);
        if (self.join_jobs.getPtr(job_id)) |state| {
            const cached = state.cached_response orelse {
                self.join_jobs_mutex.unlock();
                return null;
            };
            if (state.expires_at_millis != 0 and state.expires_at_millis <= now_ms) {
                self.join_jobs_mutex.unlock();
                self.cleanupExpiredJoinJobs();
                return null;
            }
            const total_partitions = state.total_partitions;
            const completed_partitions = state.completed_partitions;
            const phase = state.phase;
            const finalizer_retries = state.finalizer_retries;
            const coordinator_finalized = state.coordinator_finalized;
            self.join_jobs_mutex.unlock();
            var result = try self.parseJoinPartitionResponse(alloc, cached);
            result.job_id = job_id;
            result.total_partitions = total_partitions;
            result.completed_partitions = completed_partitions;
            result.job_phase = phase;
            result.finalizer_retries = finalizer_retries;
            result.coordinator_finalized = coordinator_finalized;
            result.expires_at_millis = state.expires_at_millis;
            return result;
        }
        self.join_jobs_mutex.unlock();

        var persisted = (try self.loadPersistedJoinJobState(alloc, job_id)) orelse return null;
        defer persisted.deinit(alloc);
        if (persisted.expires_at_millis != 0 and persisted.expires_at_millis <= now_ms) {
            self.deletePersistedJoinJobState(alloc, job_id);
            self.clearSharedJoinShuffleLease(job_id);
            return null;
        }
        const cached = persisted.cached_response orelse return null;

        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const entry = try self.join_jobs.getOrPut(self.alloc, job_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = .{
                .owner_group_id = persisted.owner_group_id,
                .phase = persisted.phase,
                .total_partitions = persisted.total_partitions,
                .completed_partitions = persisted.completed_partitions,
                .worker_retries = persisted.worker_retries,
                .finalizer_retries = persisted.finalizer_retries,
                .coordinator_finalized = persisted.coordinator_finalized,
                .last_updated_at_millis = persisted.last_updated_at_millis,
                .last_error = if (persisted.last_error) |value| try self.alloc.dupe(u8, value) else null,
                .cached_response = try self.alloc.dupe(u8, cached),
            };
        }
        var result = try self.parseJoinPartitionResponse(alloc, cached);
        result.job_id = job_id;
        result.total_partitions = persisted.total_partitions;
        result.completed_partitions = persisted.completed_partitions;
        result.job_phase = persisted.phase;
        result.finalizer_retries = persisted.finalizer_retries;
        result.coordinator_finalized = persisted.coordinator_finalized;
        result.expires_at_millis = persisted.expires_at_millis;
        return result;
    }

    pub fn loadJoinJobResumeState(self: *JoinJobStore, alloc: std.mem.Allocator, job_id: u64) !?JoinShuffleResumeState {
        self.cleanupExpiredJoinJobs();
        const now_ms = joinJobNowMillis();
        lockAtomic(&self.join_jobs_mutex);
        if (self.join_jobs.getPtr(job_id)) |state| {
            const partial = state.partial_response orelse {
                self.join_jobs_mutex.unlock();
                return null;
            };
            if (state.cached_response != null or state.phase == .succeeded or state.phase == .failed) {
                self.join_jobs_mutex.unlock();
                return null;
            }
            if (state.expires_at_millis != 0 and state.expires_at_millis <= now_ms) {
                self.join_jobs_mutex.unlock();
                self.cleanupExpiredJoinJobs();
                return null;
            }
            const next_partition_index = state.next_partition_index;
            self.join_jobs_mutex.unlock();
            return .{
                .result = try self.parseJoinPartitionResponse(alloc, partial),
                .next_partition_index = next_partition_index,
            };
        }
        self.join_jobs_mutex.unlock();

        var persisted = (try self.loadPersistedJoinJobState(alloc, job_id)) orelse return null;
        defer persisted.deinit(alloc);
        if (persisted.cached_response != null or persisted.phase == .succeeded or persisted.phase == .failed) return null;
        const partial = persisted.partial_response orelse return null;
        if (persisted.expires_at_millis != 0 and persisted.expires_at_millis <= now_ms) {
            self.deletePersistedJoinJobState(alloc, job_id);
            self.clearSharedJoinShuffleLease(job_id);
            return null;
        }

        var result = try self.parseJoinPartitionResponse(alloc, partial);
        errdefer result.deinit(alloc);
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const entry = try self.join_jobs.getOrPut(self.alloc, job_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = .{
                .owner_group_id = persisted.owner_group_id,
                .phase = persisted.phase,
                .total_partitions = persisted.total_partitions,
                .completed_partitions = persisted.completed_partitions,
                .next_partition_index = persisted.next_partition_index,
                .worker_retries = persisted.worker_retries,
                .finalizer_retries = persisted.finalizer_retries,
                .coordinator_finalized = persisted.coordinator_finalized,
                .last_updated_at_millis = persisted.last_updated_at_millis,
                .expires_at_millis = persisted.expires_at_millis,
                .last_error = if (persisted.last_error) |value| try self.alloc.dupe(u8, value) else null,
                .partial_response = try self.alloc.dupe(u8, partial),
                .cached_response = null,
            };
        }
        return .{
            .result = result,
            .next_partition_index = persisted.next_partition_index,
        };
    }

    pub fn loadJoinJobStateSnapshot(self: *JoinJobStore, alloc: std.mem.Allocator, job_id: u64) !?[]u8 {
        self.cleanupExpiredJoinJobs();
        const now_ms = joinJobNowMillis();
        lockAtomic(&self.join_jobs_mutex);
        if (self.join_jobs.getPtr(job_id)) |state| {
            if (state.expires_at_millis != 0 and state.expires_at_millis <= now_ms) {
                self.join_jobs_mutex.unlock();
                self.cleanupExpiredJoinJobs();
                return null;
            }
            const encoded = try encodeJoinJobState(alloc, job_id, state.*);
            self.join_jobs_mutex.unlock();
            return encoded;
        }
        self.join_jobs_mutex.unlock();

        var persisted = (try self.loadPersistedJoinJobState(alloc, job_id)) orelse return null;
        defer persisted.deinit(alloc);
        if (persisted.expires_at_millis != 0 and persisted.expires_at_millis <= now_ms) {
            self.deletePersistedJoinJobState(alloc, job_id);
            self.clearSharedJoinShuffleLease(job_id);
            return null;
        }
        return try encodeJoinJobState(alloc, job_id, persisted);
    }

    pub fn installJoinJobStateSnapshot(self: *JoinJobStore, alloc: std.mem.Allocator, job_id: u64, body: []const u8) !void {
        var parsed = try parseJoinJobState(alloc, body);
        errdefer parsed.deinit(alloc);
        defer parsed.deinit(alloc);
        lockAtomic(&self.join_jobs_mutex);
        defer self.join_jobs_mutex.unlock();
        const entry = try self.join_jobs.getOrPut(self.alloc, job_id);
        if (entry.found_existing) {
            entry.value_ptr.deinit(self.alloc);
        }
        entry.value_ptr.* = .{
            .owner_group_id = parsed.owner_group_id,
            .phase = parsed.phase,
            .total_partitions = parsed.total_partitions,
            .completed_partitions = parsed.completed_partitions,
            .next_partition_index = parsed.next_partition_index,
            .worker_retries = parsed.worker_retries,
            .finalizer_retries = parsed.finalizer_retries,
            .coordinator_finalized = parsed.coordinator_finalized,
            .last_updated_at_millis = parsed.last_updated_at_millis,
            .expires_at_millis = parsed.expires_at_millis,
            .last_error = if (parsed.last_error) |value| try self.alloc.dupe(u8, value) else null,
            .partial_response = if (parsed.partial_response) |value| try self.alloc.dupe(u8, value) else null,
            .cached_response = if (parsed.cached_response) |value| try self.alloc.dupe(u8, value) else null,
        };
        try self.persistJoinJobState(job_id, entry.value_ptr.*);
    }

    // -- encoding/decoding for partition responses --

    pub fn encodeJoinPartitionResponse(
        self: *const JoinJobStore,
        alloc: std.mem.Allocator,
        result: JoinPartitionExecutionResult,
    ) ![]u8 {
        _ = self;
        var root = std.json.Value{ .object = std.json.ObjectMap.empty };
        defer deinitJsonValue(alloc, &root);

        var hits_value = std.json.Value{ .array = std.json.Array.init(alloc) };
        errdefer deinitJsonValue(alloc, &hits_value);
        for (result.hits) |hit| {
            try hits_value.array.append(try cloneJsonValue(alloc, hit));
        }
        try putOwnedJsonField(alloc, &root.object, "hits", hits_value);

        var stats_value = std.json.Value{ .object = std.json.ObjectMap.empty };
        errdefer deinitJsonValue(alloc, &stats_value);
        try putOwnedJsonField(alloc, &stats_value.object, "left_rows_scanned", .{ .integer = result.stats.left_rows_scanned });
        try putOwnedJsonField(alloc, &stats_value.object, "right_rows_scanned", .{ .integer = result.stats.right_rows_scanned });
        try putOwnedJsonField(alloc, &stats_value.object, "rows_matched", .{ .integer = result.stats.rows_matched });
        try putOwnedJsonField(alloc, &stats_value.object, "rows_unmatched_left", .{ .integer = result.stats.rows_unmatched_left });
        try putOwnedJsonField(alloc, &stats_value.object, "rows_unmatched_right", .{ .integer = result.stats.rows_unmatched_right });
        try putOwnedJsonField(alloc, &root.object, "stats", stats_value);
        var matched_right_ids_value = std.json.Value{ .array = std.json.Array.init(alloc) };
        errdefer deinitJsonValue(alloc, &matched_right_ids_value);
        for (result.matched_right_ids) |matched_id| {
            try matched_right_ids_value.array.append(.{ .string = try alloc.dupe(u8, matched_id) });
        }
        try putOwnedJsonField(alloc, &root.object, "matched_right_ids", matched_right_ids_value);
        if (result.job_id) |value| try putOwnedJsonU64Field(alloc, &root.object, "job_id", value);
        if (result.job_phase) |value| try putOwnedJsonField(alloc, &root.object, "job_phase", .{ .string = try alloc.dupe(u8, phaseString(value)) });
        try putOwnedJsonField(alloc, &root.object, "total_partitions", .{ .integer = @intCast(result.total_partitions) });
        try putOwnedJsonField(alloc, &root.object, "completed_partitions", .{ .integer = @intCast(result.completed_partitions) });
        try putOwnedJsonField(alloc, &root.object, "expires_at_millis", .{ .integer = @intCast(result.expires_at_millis) });
        try putOwnedJsonField(alloc, &root.object, "worker_retries", .{ .integer = @intCast(result.worker_retries) });
        try putOwnedJsonField(alloc, &root.object, "finalizer_retries", .{ .integer = @intCast(result.finalizer_retries) });
        if (result.finalizer_group_id) |group_id| {
            try putOwnedJsonU64Field(alloc, &root.object, "finalizer_group_id", group_id);
        }
        try putOwnedJsonField(alloc, &root.object, "coordinator_finalized", .{ .bool = result.coordinator_finalized });
        if (result.imported_owner_group_id) |group_id| {
            try putOwnedJsonU64Field(alloc, &root.object, "imported_owner_group_id", group_id);
        }
        try putOwnedJsonField(alloc, &root.object, "imported_partial_state", .{ .bool = result.imported_partial_state });
        try putOwnedJsonField(alloc, &root.object, "imported_cached_result", .{ .bool = result.imported_cached_result });
        var attempts_value = std.json.Value{ .array = std.json.Array.init(alloc) };
        errdefer deinitJsonValue(alloc, &attempts_value);
        for (result.worker_attempts) |attempt| {
            var attempt_obj = std.json.ObjectMap.empty;
            try putOwnedJsonField(alloc, &attempt_obj, "partition_index", .{ .integer = @intCast(attempt.partition_index) });
            try putOwnedJsonU64Field(alloc, &attempt_obj, "worker_group_id", attempt.worker_group_id);
            try putOwnedJsonField(alloc, &attempt_obj, "succeeded", .{ .bool = attempt.succeeded });
            try attempts_value.array.append(.{ .object = attempt_obj });
        }
        try putOwnedJsonField(alloc, &root.object, "worker_attempts", attempts_value);
        var finalizer_attempts_value = std.json.Value{ .array = std.json.Array.init(alloc) };
        errdefer deinitJsonValue(alloc, &finalizer_attempts_value);
        for (result.finalizer_attempts) |attempt| {
            var attempt_obj = std.json.ObjectMap.empty;
            try putOwnedJsonU64Field(alloc, &attempt_obj, "worker_group_id", attempt.worker_group_id);
            try putOwnedJsonField(alloc, &attempt_obj, "succeeded", .{ .bool = attempt.succeeded });
            try finalizer_attempts_value.array.append(.{ .object = attempt_obj });
        }
        try putOwnedJsonField(alloc, &root.object, "finalizer_attempts", finalizer_attempts_value);
        return try stringifyJsonValueAlloc(alloc, root);
    }

    pub fn parseJoinPartitionResponse(
        self: *const JoinJobStore,
        alloc: std.mem.Allocator,
        body: []const u8,
    ) !JoinPartitionExecutionResult {
        _ = self;
        var parsed = try std.json.parseFromSlice(EncodedJoinPartitionResponse, alloc, body, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();
        const owned_hits = try alloc.alloc(std.json.Value, parsed.value.hits.len);
        errdefer alloc.free(owned_hits);
        for (parsed.value.hits, 0..) |item, i| {
            owned_hits[i] = try cloneJsonValue(alloc, item);
        }

        const matched_right_ids: [][]u8 = if (parsed.value.matched_right_ids) |value| blk: {
            const ids = try alloc.alloc([]u8, value.len);
            errdefer {
                for (ids[0..]) |id| alloc.free(id);
                alloc.free(ids);
            }
            for (value, 0..) |item, i| {
                ids[i] = try alloc.dupe(u8, item);
            }
            break :blk ids;
        } else &.{};

        const worker_attempts: []JoinPartitionExecutionResult.WorkerAttempt = if (parsed.value.worker_attempts) |value| blk: {
            const attempts = try alloc.alloc(JoinPartitionExecutionResult.WorkerAttempt, value.len);
            errdefer alloc.free(attempts);
            for (value, 0..) |item, i| {
                attempts[i] = .{
                    .partition_index = std.math.cast(usize, item.partition_index) orelse return error.InvalidQueryRequest,
                    .worker_group_id = item.worker_group_id,
                    .succeeded = item.succeeded,
                };
            }
            break :blk attempts;
        } else &.{};
        errdefer if (worker_attempts.len > 0) alloc.free(worker_attempts);

        const finalizer_attempts: []JoinPartitionExecutionResult.FinalizerAttempt = if (parsed.value.finalizer_attempts) |value| blk: {
            const attempts = try alloc.alloc(JoinPartitionExecutionResult.FinalizerAttempt, value.len);
            errdefer alloc.free(attempts);
            for (value, 0..) |item, i| {
                attempts[i] = .{
                    .worker_group_id = item.worker_group_id,
                    .succeeded = item.succeeded,
                };
            }
            break :blk attempts;
        } else &.{};
        errdefer if (finalizer_attempts.len > 0) alloc.free(finalizer_attempts);

        return .{
            .hits = owned_hits,
            .stats = .{
                .left_rows_scanned = parsed.value.stats.left_rows_scanned,
                .right_rows_scanned = parsed.value.stats.right_rows_scanned,
                .rows_matched = parsed.value.stats.rows_matched,
                .rows_unmatched_left = parsed.value.stats.rows_unmatched_left,
                .rows_unmatched_right = parsed.value.stats.rows_unmatched_right,
            },
            .matched_right_ids = matched_right_ids,
            .job_id = parsed.value.job_id,
            .job_phase = if (parsed.value.job_phase) |value| try phaseFromString(value) else null,
            .total_partitions = if (parsed.value.total_partitions) |value|
                std.math.cast(usize, value) orelse return error.InvalidQueryRequest
            else
                0,
            .completed_partitions = if (parsed.value.completed_partitions) |value|
                std.math.cast(usize, value) orelse return error.InvalidQueryRequest
            else
                0,
            .expires_at_millis = parsed.value.expires_at_millis orelse 0,
            .worker_retries = if (parsed.value.worker_retries) |value|
                std.math.cast(usize, value) orelse return error.InvalidQueryRequest
            else
                0,
            .finalizer_retries = if (parsed.value.finalizer_retries) |value|
                std.math.cast(usize, value) orelse return error.InvalidQueryRequest
            else
                0,
            .finalizer_group_id = parsed.value.finalizer_group_id,
            .coordinator_finalized = parsed.value.coordinator_finalized orelse false,
            .imported_owner_group_id = parsed.value.imported_owner_group_id,
            .imported_partial_state = parsed.value.imported_partial_state orelse false,
            .imported_cached_result = parsed.value.imported_cached_result orelse false,
            .worker_attempts = worker_attempts,
            .finalizer_attempts = finalizer_attempts,
        };
    }

    pub fn snapshotJoinJobProgressResult(
        self: *JoinJobStore,
        alloc: std.mem.Allocator,
        hits: []const std.json.Value,
        stats: JoinedQueryStats,
        groups_queried: usize,
        matched_right_ids: *const std.StringHashMapUnmanaged(void),
        job_id: u64,
        total_partitions: usize,
        completed_partitions: usize,
        worker_retries: usize,
        worker_attempts: []const JoinPartitionExecutionResult.WorkerAttempt,
    ) !JoinPartitionExecutionResult {
        return try buildJoinPartitionExecutionResultAlloc(
            alloc,
            hits,
            stats,
            groups_queried,
            matched_right_ids,
            job_id,
            .finalizing,
            total_partitions,
            completed_partitions,
            self.joinJobExpiryForPhase(.finalizing, joinJobNowMillis()),
            worker_retries,
            worker_attempts,
        );
    }
};

// ---------------------------------------------------------------------------
// Execution functions (top-level pub)
// ---------------------------------------------------------------------------

pub fn executeSupportedJoinedPublicTableQueryRequest(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    table_name: []const u8,
    body: []const u8,
    row_filter_json: ?[]const u8,
    join: SupportedJoinRequest,
    foreign_sources: foreign_mod.PostgresSourceMap,
) (public_table_http.TableApi.ExecuteQueryError || error{OutOfMemory})![]u8 {
    const uses_foreign = joinUsesForeignSource(join, foreign_sources);
    var contract_request = metadata_openapi.server.parseQueryTableBody(alloc, body) catch return error.InvalidQueryRequest;
    defer contract_request.deinit();
    const requested_left_field_strings = contract_request.value.fields orelse &.{};
    const requested_left_fields = alloc.alloc(std.json.Value, requested_left_field_strings.len) catch return error.InternalFailure;
    defer alloc.free(requested_left_fields);
    for (requested_left_field_strings, 0..) |field, i| {
        requested_left_fields[i] = .{ .string = field };
    }
    if (contract_request.value.count == true) return error.InvalidQueryRequest;
    const rewrite = rewriteJoinedBaseQueryBodyAlloc(alloc, contract_request.value, join.left_field) catch {
        return error.InternalFailure;
    };
    const appended_left_field = rewrite.appended_left_field;
    const primary_body = rewrite.body;
    defer alloc.free(primary_body);

    var primary_result = ctx.executePlainQuery(alloc, source, table_name, primary_body, row_filter_json) catch |err| switch (err) {
        error.InvalidQueryRequest => return error.InvalidQueryRequest,
        error.TableNotFound => return error.NotFound,
        else => return error.InternalFailure,
    };
    defer primary_result.deinit(alloc);

    var owned_response = json_helpers.parseOwnedJsonValueAlloc(alloc, primary_result.json) catch return error.InternalFailure;
    defer deinitJsonValue(alloc, &owned_response);
    const hits_ptr = queryHitsArrayPtr(&owned_response) catch return error.InternalFailure;
    if (hits_ptr.items.len == 0) return alloc.dupe(u8, primary_result.json) catch return error.InternalFailure;
    const plan = planSupportedJoinExecution(ctx, alloc, table_name, join, hits_ptr.items, foreign_sources) catch |err| switch (err) {
        error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
        else => return error.InternalFailure,
    };

    if (!uses_foreign and plan.strategy == .shuffle and join.nested_join == null) {
        if (executeSupportedDistributedJoinFinalized(ctx, job_store, alloc, source, join, hits_ptr.items, requested_left_fields, appended_left_field, plan) catch |err| switch (err) {
            error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
            error.TableNotFound => return error.NotFound,
            else => {
                std.log.err("distributed shuffle join failed table={s} err={}", .{ table_name, err });
                return error.InternalFailure;
            },
        }) |distributed_join_value| {
            var distributed_join = distributed_join_value;
            defer distributed_join.deinit(alloc);
            try join_model.applyJoinShellToResponse(alloc, &owned_response, distributed_join);
            try maybeAttachJoinProfile(alloc, &owned_response, distributed_join.stats, plan, .shuffle, true, distributed_join.groups_queried);
            try maybeAttachJoinWorkerExecution(alloc, &owned_response, distributed_join);
            return stringifyJsonValueAlloc(alloc, owned_response) catch error.InternalFailure;
        }
    }

    var right_result = executeSupportedRightJoinQuery(ctx, job_store, alloc, source, join, hits_ptr.items, plan, foreign_sources) catch |err| switch (err) {
        error.InvalidQueryRequest, error.UnsupportedQueryRequest => return error.InvalidQueryRequest,
        error.TableNotFound => return error.NotFound,
        else => return error.InternalFailure,
    };
    defer right_result.deinit(alloc);
    const stats = applyJoinedRightHitsToResponse(
        alloc,
        &owned_response,
        hits_ptr.items,
        join,
        right_result.hits,
        requested_left_fields,
        appended_left_field,
    ) catch |err| switch (err) {
        error.InvalidQueryRequest => return error.InvalidQueryRequest,
        else => return error.InternalFailure,
    };
    try maybeAttachJoinProfile(alloc, &owned_response, stats, plan, right_result.strategy_used, right_result.distributed_execution, right_result.groups_queried);
    return stringifyJsonValueAlloc(alloc, owned_response) catch error.InternalFailure;
}

pub fn executeSupportedDistributedJoinFinalized(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    left_fields: []const std.json.Value,
    appended_left_field: bool,
    plan: PlannedJoinExecution,
) !?JoinPartitionExecutionResult {
    const engine: StatefulDistributedShuffleEngine = .{
        .ctx = ctx,
        .job_store = job_store,
        .alloc = alloc,
        .source = source,
        .join = join,
        .left_hits = left_hits,
        .appended_left_field = appended_left_field,
        .plan = plan,
    };
    return try engine.executeFinalizedAlloc(left_fields);
}

fn applyDistributedFinalizerResultMetadata(
    alloc: std.mem.Allocator,
    result: *JoinPartitionExecutionResult,
    job_id: ?u64,
    finalizer_group_id: ?u64,
    retry_delta: usize,
    execution_mode: JoinShuffleExecutionMode,
    finalizer_attempts: []JoinPartitionExecutionResult.FinalizerAttempt,
    coordinator_finalized: bool,
) !void {
    errdefer if (finalizer_attempts.len > 0) alloc.free(finalizer_attempts);

    if (result.job_id == null) result.job_id = job_id;
    if (result.finalizer_group_id == null) result.finalizer_group_id = finalizer_group_id;
    result.finalizer_retries += retry_delta;
    result.execution_mode = execution_mode;
    result.coordinator_finalized = coordinator_finalized;
    if (result.finalizer_attempts.len > 0) alloc.free(result.finalizer_attempts);
    result.finalizer_attempts = finalizer_attempts;
}

const StatefulShuffleFinalizerState = struct {
    stable_job_id: u64,
    durable: bool,
    durable_job_id: ?u64,
    previous_owner_group_id: ?u64,
    execution_mode: JoinShuffleExecutionMode,
    finalizer_attempts: std.ArrayListUnmanaged(JoinPartitionExecutionResult.FinalizerAttempt) = .empty,

    fn init(
        ctx: JoinContext,
        job_store: *JoinJobStore,
        stable_job_id: u64,
        plan: PlannedJoinExecution,
        left_hit_count: usize,
        worker_group_count: usize,
    ) StatefulShuffleFinalizerState {
        const durable = job_store.shouldUseDurableDistributedJoin(plan, left_hit_count, worker_group_count);
        const previous_owner_group_id = if (durable) blk: {
            const lease = ctx.getJoinShuffleLease(stable_job_id) catch null;
            break :blk if (lease) |value| value.owner_group_id else null;
        } else null;
        return .{
            .stable_job_id = stable_job_id,
            .durable = durable,
            .durable_job_id = if (durable) stable_job_id else null,
            .previous_owner_group_id = previous_owner_group_id,
            .execution_mode = if (durable) .durable else .transient,
        };
    }

    fn deinit(self: *StatefulShuffleFinalizerState, alloc: std.mem.Allocator) void {
        self.finalizer_attempts.deinit(alloc);
        self.* = undefined;
    }

    fn maybeLoadImportedCachedResult(
        self: *const StatefulShuffleFinalizerState,
        job_store: *JoinJobStore,
        alloc: std.mem.Allocator,
        source: table_reads.TableReadSource,
        table_name: []const u8,
    ) !?JoinPartitionExecutionResult {
        if (!self.durable or self.previous_owner_group_id == null) return null;
        if (try job_store.loadJoinJobCachedResult(alloc, self.stable_job_id)) |cached| return cached;
        if (try tryImportRemoteJoinJobState(job_store, alloc, source, self.previous_owner_group_id.?, table_name, self.stable_job_id)) {
            if (try job_store.loadJoinJobCachedResult(alloc, self.stable_job_id)) |cached_value| {
                var cached = cached_value;
                cached.imported_owner_group_id = self.previous_owner_group_id.?;
                cached.imported_cached_result = true;
                return cached;
            }
        }
        return null;
    }

    fn startIndex(
        self: *const StatefulShuffleFinalizerState,
        job_store: *JoinJobStore,
        worker_group_ids: []const u64,
    ) usize {
        return if (self.durable)
            job_store.sharedJoinShuffleFinalizerStartIndex(self.stable_job_id, worker_group_ids)
        else
            preferredFinalizerStartIndex(self.stable_job_id, worker_group_ids);
    }

    fn handoffOwnerGroupId(self: *const StatefulShuffleFinalizerState, finalizer_group_id: u64) ?u64 {
        return if (self.previous_owner_group_id) |owner|
            if (owner != finalizer_group_id) owner else null
        else
            null;
    }

    fn recordAttempt(
        self: *StatefulShuffleFinalizerState,
        alloc: std.mem.Allocator,
        worker_group_id: u64,
        succeeded: bool,
    ) !void {
        try self.finalizer_attempts.append(alloc, .{
            .worker_group_id = worker_group_id,
            .succeeded = succeeded,
        });
    }

    fn finalizeResult(
        self: *StatefulShuffleFinalizerState,
        alloc: std.mem.Allocator,
        job_store: *JoinJobStore,
        result: *JoinPartitionExecutionResult,
        finalizer_group_id: ?u64,
        retry_delta: usize,
        coordinator_finalized: bool,
    ) !void {
        try applyDistributedFinalizerResultMetadata(
            alloc,
            result,
            self.durable_job_id,
            finalizer_group_id,
            retry_delta,
            self.execution_mode,
            try self.finalizer_attempts.toOwnedSlice(alloc),
            coordinator_finalized,
        );
        self.finalizer_attempts = .empty;
        if (self.durable) {
            job_store.syncSharedJoinShuffleLease(self.stable_job_id, result.finalizer_group_id, .succeeded);
        }
    }
};

const StatefulShufflePartitionState = struct {
    joined_hits: std.json.Array,
    seen_groups: std.AutoHashMapUnmanaged(u64, void) = .{},
    worker_attempts: std.ArrayListUnmanaged(JoinPartitionExecutionResult.WorkerAttempt) = .empty,
    matched_right_ids: std.StringHashMapUnmanaged(void) = .{},
    stats: JoinedQueryStats = .{},
    worker_retries: usize = 0,
    completed_partitions: usize = 0,
    next_partition_index: usize = 0,
    imported_owner_group_id: ?u64 = null,
    imported_partial_state: bool = false,
    imported_cached_result: bool = false,

    fn init(alloc: std.mem.Allocator) StatefulShufflePartitionState {
        return .{
            .joined_hits = std.json.Array.init(alloc),
        };
    }

    fn deinit(self: *StatefulShufflePartitionState, alloc: std.mem.Allocator) void {
        for (self.joined_hits.items) |*item| deinitJsonValue(alloc, item);
        self.joined_hits.deinit();
        self.seen_groups.deinit(alloc);
        self.worker_attempts.deinit(alloc);
        self.matched_right_ids.deinit(alloc);
        self.* = undefined;
    }

    fn restoreAlloc(
        self: *StatefulShufflePartitionState,
        alloc: std.mem.Allocator,
        resume_value: JoinShuffleResumeState,
    ) !void {
        for (resume_value.result.hits) |item| {
            try self.joined_hits.append(try cloneJsonValue(alloc, item));
        }
        self.stats = resume_value.result.stats;
        self.worker_retries = resume_value.result.worker_retries;
        self.completed_partitions = resume_value.result.completed_partitions;
        self.next_partition_index = resume_value.next_partition_index;
        self.imported_owner_group_id = resume_value.result.imported_owner_group_id;
        self.imported_partial_state = resume_value.result.imported_partial_state;
        self.imported_cached_result = resume_value.result.imported_cached_result;
        for (resume_value.result.worker_attempts) |attempt| {
            try self.worker_attempts.append(alloc, attempt);
            try self.seen_groups.put(alloc, attempt.worker_group_id, {});
        }
        for (resume_value.result.matched_right_ids) |matched_id| {
            try self.matched_right_ids.put(alloc, matched_id, {});
        }
    }

    fn groupsQueried(self: *const StatefulShufflePartitionState) usize {
        return self.seen_groups.count();
    }

    fn markPartitionCompleted(self: *StatefulShufflePartitionState, partition_index: usize) void {
        self.completed_partitions = partition_index + 1;
    }

    fn applyWorkerResultAlloc(
        self: *StatefulShufflePartitionState,
        alloc: std.mem.Allocator,
        worker_result: *const JoinPartitionExecutionResult,
    ) !void {
        self.stats.left_rows_scanned += worker_result.stats.left_rows_scanned;
        self.stats.right_rows_scanned += worker_result.stats.right_rows_scanned;
        self.stats.rows_matched += worker_result.stats.rows_matched;
        self.stats.rows_unmatched_left += worker_result.stats.rows_unmatched_left;
        self.stats.rows_unmatched_right += worker_result.stats.rows_unmatched_right;
        for (worker_result.matched_right_ids) |matched_id| {
            try self.matched_right_ids.put(alloc, matched_id, {});
        }
        try appendClonedJsonHitsToArray(alloc, &self.joined_hits, worker_result.hits);
    }

    fn recordProgress(
        self: *const StatefulShufflePartitionState,
        job_store: *JoinJobStore,
        alloc: std.mem.Allocator,
        maybe_job_id: ?u64,
        partition_count: usize,
    ) !void {
        const job_id = maybe_job_id orelse return;
        var partial_result = try job_store.snapshotJoinJobProgressResult(
            alloc,
            self.joined_hits.items,
            self.stats,
            self.groupsQueried(),
            &self.matched_right_ids,
            job_id,
            partition_count,
            self.completed_partitions,
            self.worker_retries,
            self.worker_attempts.items,
        );
        defer partial_result.deinit(alloc);
        try job_store.recordJoinJobProgress(job_id, self.completed_partitions, partial_result);
    }

    fn buildResultAlloc(
        self: *const StatefulShufflePartitionState,
        alloc: std.mem.Allocator,
        job_store: *JoinJobStore,
        job_id: ?u64,
        partition_count: usize,
    ) !JoinPartitionExecutionResult {
        var result = try buildJoinPartitionExecutionResultAlloc(
            alloc,
            self.joined_hits.items,
            self.stats,
            self.groupsQueried(),
            &self.matched_right_ids,
            job_id,
            if (job_id != null) .finalizing else null,
            partition_count,
            self.completed_partitions,
            if (job_id != null) job_store.joinJobExpiryForPhase(.finalizing, joinJobNowMillis()) else 0,
            self.worker_retries,
            self.worker_attempts.items,
        );
        result.imported_owner_group_id = self.imported_owner_group_id;
        result.imported_partial_state = self.imported_partial_state;
        result.imported_cached_result = self.imported_cached_result;
        return result;
    }
};

const DistributedJoinPartitionDispatch = struct {
    result: ?JoinPartitionExecutionResult = null,
    retry_delta: usize = 0,
};

const DistributedRightJoinGroups = struct {
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    snapshot: metadata_api.AdminSnapshot,
    table_id: u64,
    group_ids: []u64,

    fn init(
        ctx: JoinContext,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        min_group_count: usize,
    ) !?DistributedRightJoinGroups {
        var snapshot = (try ctx.adminSnapshot()) orelse return null;
        errdefer ctx.freeAdminSnapshot(&snapshot);
        const right_table = tables_api.findTableByName(&snapshot, table_name) orelse return null;
        const group_ids = try rightJoinGroupIdsFromSnapshot(alloc, &snapshot, right_table.table_id);
        errdefer alloc.free(group_ids);
        if (group_ids.len < min_group_count) return null;
        return .{
            .ctx = ctx,
            .alloc = alloc,
            .snapshot = snapshot,
            .table_id = right_table.table_id,
            .group_ids = group_ids,
        };
    }

    fn deinit(self: *DistributedRightJoinGroups) void {
        self.alloc.free(self.group_ids);
        self.ctx.freeAdminSnapshot(&self.snapshot);
        self.* = undefined;
    }
};

const StatefulShufflePreparedJob = union(enum) {
    cached_result: JoinPartitionExecutionResult,
    resume_state: JoinShuffleResumeState,
    fresh: void,

    fn deinit(self: *StatefulShufflePreparedJob, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .cached_result => |*result| result.deinit(alloc),
            .resume_state => |*resume_state| resume_state.deinit(alloc),
            .fresh => {},
        }
        self.* = undefined;
    }
};

const StatefulShuffleJobLifecycle = struct {
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    table_name: []const u8,
    finalizer_group_id: u64,
    job_id: ?u64,

    fn prepareAlloc(
        self: StatefulShuffleJobLifecycle,
        join: SupportedJoinRequest,
        shuffle_partitions: usize,
        handoff_owner_group_id: ?u64,
    ) !StatefulShufflePreparedJob {
        const job_id = self.job_id orelse return .fresh;
        if (try self.job_store.loadJoinJobCachedResult(self.alloc, job_id)) |result| {
            return .{ .cached_result = result };
        }

        if (try self.job_store.loadJoinJobResumeState(self.alloc, job_id)) |resume_state| {
            return .{ .resume_state = resume_state };
        }

        if (handoff_owner_group_id) |handoff_group_id| {
            if (handoff_group_id != 0 and handoff_group_id != self.finalizer_group_id) {
                if (try tryImportRemoteJoinJobState(self.job_store, self.alloc, self.source, handoff_group_id, join.right_table, job_id)) {
                    if (try self.job_store.loadJoinJobCachedResult(self.alloc, job_id)) |result_value| {
                        var result = result_value;
                        result.imported_owner_group_id = handoff_group_id;
                        result.imported_cached_result = true;
                        return .{ .cached_result = result };
                    }
                    if (try self.job_store.loadJoinJobResumeState(self.alloc, job_id)) |resume_state_value| {
                        var resume_state = resume_state_value;
                        resume_state.result.imported_owner_group_id = handoff_group_id;
                        resume_state.result.imported_partial_state = true;
                        return .{ .resume_state = resume_state };
                    }
                }
            }
        }

        try self.job_store.recordJoinJobStart(
            job_id,
            if (self.finalizer_group_id != 0) self.finalizer_group_id else null,
            shuffle_partitions,
        );
        return .fresh;
    }

    fn recordFailure(self: StatefulShuffleJobLifecycle, err: anyerror) !void {
        const job_id = self.job_id orelse return;
        try self.job_store.recordJoinJobFailed(job_id, err);
    }

    fn finalizeSucceeded(
        self: StatefulShuffleJobLifecycle,
        result: *JoinPartitionExecutionResult,
        shuffle_partitions: usize,
    ) !void {
        result.execution_mode = if (self.job_id != null) .durable else .transient;
        result.job_id = self.job_id;
        result.job_phase = if (self.job_id != null) .finalizing else null;
        result.total_partitions = shuffle_partitions;
        result.expires_at_millis = if (self.job_id != null)
            self.job_store.joinJobExpiryForPhase(.finalizing, joinJobNowMillis())
        else
            0;
        if (self.finalizer_group_id != 0) result.finalizer_group_id = self.finalizer_group_id;
        if (self.job_id) |job_id| {
            const encoded = try self.job_store.encodeJoinPartitionResponse(self.alloc, result.*);
            defer self.alloc.free(encoded);
            try self.job_store.recordJoinJobSucceeded(
                job_id,
                result.finalizer_group_id,
                result.finalizer_retries,
                result.coordinator_finalized,
                encoded,
            );
            self.job_store.syncSharedJoinShuffleLease(job_id, result.finalizer_group_id, .succeeded);
            result.job_phase = .succeeded;
            result.completed_partitions = result.total_partitions;
            result.expires_at_millis = self.job_store.joinJobExpiryForPhase(.succeeded, joinJobNowMillis());
        }
    }
};

const PlannedRightJoinExecutionCoordinator = struct {
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    plan: PlannedJoinExecution,
    foreign_sources: foreign_mod.PostgresSourceMap,
    allow_distributed: bool,

    fn execute(self: PlannedRightJoinExecutionCoordinator) !RightJoinQueryResult {
        if (!self.allow_distributed and join_model.joinSupportsIndexLookup(self.join)) {
            return try executeRightJoinLookupQuery(self.alloc, self.source, self.join, self.left_hits);
        }
        return switch (self.plan.strategy) {
            .index_lookup => try self.executeIndexLookup(),
            .shuffle => try self.executeShuffle(),
            .broadcast => try self.executeBroadcast(),
        };
    }

    fn executeIndexLookup(self: PlannedRightJoinExecutionCoordinator) !RightJoinQueryResult {
        if (self.allow_distributed) {
            if (try executeRightJoinDistributedLookupQuery(self.ctx, self.alloc, self.source, self.join, self.left_hits)) |distributed| {
                return distributed;
            }
        }
        return try executeRightJoinLookupQuery(self.alloc, self.source, self.join, self.left_hits);
    }

    fn executeShuffle(self: PlannedRightJoinExecutionCoordinator) !RightJoinQueryResult {
        if (self.allow_distributed) {
            if (try executeRightJoinDistributedShuffleQuery(self.ctx, self.alloc, self.source, self.join, self.left_hits, self.plan)) |distributed| {
                return distributed;
            }
        }
        return try executeRightJoinShuffleQuery(
            self.ctx,
            self.job_store,
            self.alloc,
            self.source,
            self.join,
            self.left_hits,
            self.plan,
            self.foreign_sources,
        );
    }

    fn executeBroadcast(self: PlannedRightJoinExecutionCoordinator) !RightJoinQueryResult {
        if (self.allow_distributed) {
            if (try executeRightJoinDistributedBroadcastQuery(self.ctx, self.alloc, self.source, self.join, self.left_hits)) |distributed| {
                return distributed;
            }
        }
        return try executeRightJoinBroadcastQueryLocal(
            self.ctx,
            self.job_store,
            self.alloc,
            self.source,
            self.join,
            self.left_hits,
            self.foreign_sources,
        );
    }
};

const StatefulDistributedShuffleEngine = struct {
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    appended_left_field: bool,
    plan: PlannedJoinExecution,

    fn loadWorkerGroupIdsAlloc(self: StatefulDistributedShuffleEngine) !?[]u64 {
        var snapshot = (try self.ctx.adminSnapshot()) orelse return null;
        defer self.ctx.freeAdminSnapshot(&snapshot);
        const right_table = tables_api.findTableByName(&snapshot, self.join.right_table) orelse return error.TableNotFound;
        const worker_group_ids = try rightJoinGroupIdsFromSnapshot(self.alloc, &snapshot, right_table.table_id);
        errdefer self.alloc.free(worker_group_ids);
        if (worker_group_ids.len <= 1) return null;
        return worker_group_ids;
    }

    fn stableJobIdAlloc(self: StatefulDistributedShuffleEngine) !u64 {
        return try self.job_store.stableDistributedJoinJobId(
            self.alloc,
            self.join,
            self.left_hits,
            self.appended_left_field,
            self.plan.shuffle_partitions,
        );
    }

    fn executeFinalizedAlloc(
        self: StatefulDistributedShuffleEngine,
        left_fields: []const std.json.Value,
    ) !?JoinPartitionExecutionResult {
        const worker_group_ids = (try self.loadWorkerGroupIdsAlloc()) orelse return null;
        defer self.alloc.free(worker_group_ids);

        const stable_job_id = try self.stableJobIdAlloc();
        var coordinator = StatefulShuffleFinalizerState.init(
            self.ctx,
            self.job_store,
            stable_job_id,
            self.plan,
            self.left_hits.len,
            worker_group_ids.len,
        );
        defer coordinator.deinit(self.alloc);

        if (try coordinator.maybeLoadImportedCachedResult(self.job_store, self.alloc, self.source, self.join.right_table)) |cached| {
            return cached;
        }

        if (try self.tryRemoteFinalizerAlloc(&coordinator, stable_job_id, worker_group_ids, left_fields)) |result| {
            return result;
        }

        return try self.executeCoordinatorFinalizerFallbackAlloc(&coordinator, stable_job_id, left_fields);
    }

    fn executePartitionsAlloc(
        self: StatefulDistributedShuffleEngine,
        job_id: ?u64,
        resume_state: ?JoinShuffleResumeState,
    ) !?JoinPartitionExecutionResult {
        const worker_group_ids = (try self.loadWorkerGroupIdsAlloc()) orelse return null;
        defer self.alloc.free(worker_group_ids);

        const partition_count = @max(@as(usize, 1), self.plan.shuffle_partitions);
        var state = StatefulShufflePartitionState.init(self.alloc);
        defer state.deinit(self.alloc);
        if (resume_state) |resume_value| {
            try state.restoreAlloc(self.alloc, resume_value);
        }

        for (state.next_partition_index..partition_count) |partition_index| {
            const partition_left_hits = try self.collectPartitionLeftHitsAlloc(partition_index, partition_count);
            defer if (partition_left_hits.len > 0) self.alloc.free(partition_left_hits);

            if (partition_left_hits.len == 0) {
                try self.finishEmptyPartitionAlloc(&state, job_id, partition_count, partition_index);
                continue;
            }

            var dispatch = try self.dispatchPartitionAcrossWorkers(
                job_id,
                partition_index,
                partition_count,
                worker_group_ids,
                partition_left_hits,
                &state,
            );
            try self.finishDispatchedPartitionAlloc(&state, job_id, partition_count, partition_index, &dispatch);
        }

        return try state.buildResultAlloc(self.alloc, self.job_store, job_id, partition_count);
    }

    fn executeFinalizerWorkerAlloc(
        self: StatefulDistributedShuffleEngine,
        finalizer_group_id: u64,
        table_name: []const u8,
        job_id: ?u64,
        left_fields: []const std.json.Value,
        handoff_owner_group_id: ?u64,
    ) !JoinPartitionExecutionResult {
        const lifecycle: StatefulShuffleJobLifecycle = .{
            .job_store = self.job_store,
            .alloc = self.alloc,
            .source = self.source,
            .table_name = table_name,
            .finalizer_group_id = finalizer_group_id,
            .job_id = job_id,
        };
        var prepared = try lifecycle.prepareAlloc(self.join, self.plan.shuffle_partitions, handoff_owner_group_id);
        defer switch (prepared) {
            .cached_result => {},
            .resume_state => |*value| value.deinit(self.alloc),
            .fresh => {},
        };

        switch (prepared) {
            .cached_result => |result| return result,
            else => {},
        }

        const resume_state = switch (prepared) {
            .resume_state => |value| value,
            .fresh => null,
            .cached_result => unreachable,
        };

        const result_opt = self.executePartitionsAlloc(job_id, resume_state) catch |err| {
            std.log.warn("distributed join finalizer worker execution failed table={s} finalizer_group_id={d} err={}", .{
                table_name,
                finalizer_group_id,
                err,
            });
            try lifecycle.recordFailure(err);
            return err;
        };
        var result = result_opt orelse {
            try lifecycle.recordFailure(error.UnknownGroup);
            return error.UnknownGroup;
        };
        return try self.completeFinalizerResultAlloc(lifecycle, &result, left_fields);
    }

    fn collectPartitionLeftHitsAlloc(
        self: StatefulDistributedShuffleEngine,
        partition_index: usize,
        partition_count: usize,
    ) ![]const std.json.Value {
        return try collectLeftHitsForJoinPartitionAlloc(
            self.alloc,
            self.left_hits,
            self.join.left_field,
            partition_index,
            partition_count,
        );
    }

    fn finishEmptyPartitionAlloc(
        self: StatefulDistributedShuffleEngine,
        state: *StatefulShufflePartitionState,
        job_id: ?u64,
        partition_count: usize,
        partition_index: usize,
    ) !void {
        state.markPartitionCompleted(partition_index);
        try state.recordProgress(self.job_store, self.alloc, job_id, partition_count);
    }

    fn dispatchPartitionAcrossWorkers(
        self: StatefulDistributedShuffleEngine,
        job_id: ?u64,
        partition_index: usize,
        partition_count: usize,
        worker_group_ids: []const u64,
        partition_left_hits: []const std.json.Value,
        state: *StatefulShufflePartitionState,
    ) !DistributedJoinPartitionDispatch {
        var out: DistributedJoinPartitionDispatch = .{};
        var last_err: ?anyerror = null;
        for (0..worker_group_ids.len) |attempt| {
            const worker_group_id = worker_group_ids[(partition_index + attempt) % worker_group_ids.len];
            try state.seen_groups.put(self.alloc, worker_group_id, {});
            out.result = dispatchJoinPartitionToWorker(
                self.ctx,
                self.job_store,
                self.alloc,
                self.source,
                job_id,
                worker_group_id,
                worker_group_ids,
                partition_index,
                partition_count,
                self.join,
                partition_left_hits,
                self.appended_left_field,
            ) catch |err| {
                std.log.warn("distributed join partition dispatch failed partition_index={d} worker_group_id={d} err={}", .{
                    partition_index,
                    worker_group_id,
                    err,
                });
                last_err = err;
                try state.worker_attempts.append(self.alloc, .{
                    .partition_index = partition_index,
                    .worker_group_id = worker_group_id,
                    .succeeded = false,
                });
                continue;
            };
            try state.worker_attempts.append(self.alloc, .{
                .partition_index = partition_index,
                .worker_group_id = worker_group_id,
                .succeeded = out.result != null,
            });
            if (out.result != null) {
                out.retry_delta = attempt;
                break;
            }
        }
        if (out.result == null and last_err != null) return last_err.?;
        return out;
    }

    fn finishDispatchedPartitionAlloc(
        self: StatefulDistributedShuffleEngine,
        state: *StatefulShufflePartitionState,
        job_id: ?u64,
        partition_count: usize,
        partition_index: usize,
        dispatch: *DistributedJoinPartitionDispatch,
    ) !void {
        state.worker_retries += dispatch.retry_delta;
        var worker_result = dispatch.result orelse return;
        defer worker_result.deinit(self.alloc);

        try state.applyWorkerResultAlloc(self.alloc, &worker_result);
        state.markPartitionCompleted(partition_index);
        try state.recordProgress(self.job_store, self.alloc, job_id, partition_count);
    }

    fn tryRemoteFinalizerAlloc(
        self: StatefulDistributedShuffleEngine,
        coordinator: *StatefulShuffleFinalizerState,
        stable_job_id: u64,
        worker_group_ids: []const u64,
        left_fields: []const std.json.Value,
    ) !?JoinPartitionExecutionResult {
        const start_index = coordinator.startIndex(self.job_store, worker_group_ids);
        for (0..worker_group_ids.len) |attempt| {
            const finalizer_group_id = worker_group_ids[(start_index + attempt) % worker_group_ids.len];
            const body = try encodeJoinFinalizeRequest(
                self.alloc,
                stable_job_id,
                coordinator.handoffOwnerGroupId(finalizer_group_id),
                self.join,
                self.left_hits,
                left_fields,
                self.appended_left_field,
                self.plan.shuffle_partitions,
            );
            defer self.alloc.free(body);
            if (try self.source.joinFinalizeGroupLocal(self.alloc, finalizer_group_id, self.join.right_table, body)) |response_value| {
                var response = response_value;
                defer response.deinit(self.alloc);
                try coordinator.recordAttempt(self.alloc, finalizer_group_id, true);
                var result = self.job_store.parseJoinPartitionResponse(self.alloc, response.json) catch |err| {
                    std.log.err("distributed join finalizer response parse failed finalizer_group_id={d} err={} body={s}", .{
                        finalizer_group_id,
                        err,
                        response.json,
                    });
                    return err;
                };
                try coordinator.finalizeResult(self.alloc, self.job_store, &result, finalizer_group_id, attempt, false);
                return result;
            }
            try coordinator.recordAttempt(self.alloc, finalizer_group_id, false);
        }
        return null;
    }

    fn executeCoordinatorFinalizerFallbackAlloc(
        self: StatefulDistributedShuffleEngine,
        coordinator: *StatefulShuffleFinalizerState,
        stable_job_id: u64,
        left_fields: []const std.json.Value,
    ) !JoinPartitionExecutionResult {
        const fallback_body = try encodeJoinFinalizeRequest(
            self.alloc,
            stable_job_id,
            coordinator.previous_owner_group_id,
            self.join,
            self.left_hits,
            left_fields,
            self.appended_left_field,
            self.plan.shuffle_partitions,
        );
        defer self.alloc.free(fallback_body);
        var result = try executeJoinFinalizeWorkerLocal(
            self.ctx,
            self.job_store,
            self.alloc,
            self.source,
            0,
            self.join.right_table,
            fallback_body,
        );
        try coordinator.finalizeResult(
            self.alloc,
            self.job_store,
            &result,
            null,
            coordinator.finalizer_attempts.items.len,
            true,
        );
        return result;
    }

    fn completeFinalizerResultAlloc(
        self: StatefulDistributedShuffleEngine,
        lifecycle: StatefulShuffleJobLifecycle,
        result: *JoinPartitionExecutionResult,
        left_fields: []const std.json.Value,
    ) !JoinPartitionExecutionResult {
        try self.applyRightJoinFinalizerCompletionAlloc(result, left_fields);
        try lifecycle.finalizeSucceeded(result, self.plan.shuffle_partitions);
        const completed = result.*;
        result.* = undefined;
        return completed;
    }

    fn applyRightJoinFinalizerCompletionAlloc(
        self: StatefulDistributedShuffleEngine,
        result: *JoinPartitionExecutionResult,
        left_fields: []const std.json.Value,
    ) !void {
        if (self.join.join_type != .right) return;
        const left_field_names = try requestedFieldNamesAlloc(self.alloc, left_fields);
        defer self.alloc.free(left_field_names);
        try appendDistributedRightJoinUnmatchedRows(
            self.ctx,
            self.job_store,
            self.alloc,
            self.source,
            self.join,
            self.left_hits,
            left_field_names,
            self.appended_left_field,
            result,
        );
    }
};

pub fn executeSupportedRightJoinQuery(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    plan: PlannedJoinExecution,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !RightJoinQueryResult {
    if (foreign_sources.get(join.right_table)) |foreign_source| {
        return try executeForeignRightJoinQuery(ctx, job_store, alloc, source, foreign_source, join, left_hits, foreign_sources);
    }
    return try executeSupportedRightJoinQueryWithMode(
        ctx,
        job_store,
        alloc,
        source,
        join,
        left_hits,
        plan,
        foreign_sources,
        true,
    );
}

pub fn executeSupportedRightJoinQueryCoordinatorOnly(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    plan: PlannedJoinExecution,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !RightJoinQueryResult {
    if (foreign_sources.get(join.right_table)) |foreign_source| {
        return try executeForeignRightJoinQuery(ctx, job_store, alloc, source, foreign_source, join, left_hits, foreign_sources);
    }
    return try executeSupportedRightJoinQueryWithMode(
        ctx,
        job_store,
        alloc,
        source,
        join,
        left_hits,
        plan,
        foreign_sources,
        false,
    );
}

fn executeSupportedRightJoinQueryWithMode(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    plan: PlannedJoinExecution,
    foreign_sources: foreign_mod.PostgresSourceMap,
    allow_distributed: bool,
) !RightJoinQueryResult {
    return try (PlannedRightJoinExecutionCoordinator{
        .ctx = ctx,
        .job_store = job_store,
        .alloc = alloc,
        .source = source,
        .join = join,
        .left_hits = left_hits,
        .plan = plan,
        .foreign_sources = foreign_sources,
        .allow_distributed = allow_distributed,
    }).execute();
}

pub fn executeForeignRightJoinQuery(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    foreign_source: foreign_mod.PostgresConfig,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !RightJoinQueryResult {
    const registry = try ctx.ensureForeignRegistry();

    const effective_fields = try buildForeignJoinFieldListAlloc(alloc, join);
    defer freeOwnedStringSlice(alloc, effective_fields);
    const raw_filter_query_json = if (join.right_filters) |filters|
        if (filters.filter_query) |query|
            try stringifyJsonValueAlloc(alloc, query)
        else
            null
    else
        null;
    defer if (raw_filter_query_json) |query| alloc.free(query);
    const filter_query_json = try foreign_sources_api.buildEffectiveFilterQueryJsonAlloc(
        alloc,
        foreign_source,
        raw_filter_query_json,
        if (join.right_filters) |filters| filters.filter_prefix else null,
    );
    defer if (filter_query_json) |query| alloc.free(query);

    var params = try foreign_source.toQueryParams(alloc, .{
        .fields = effective_fields,
        .filter_query_json = filter_query_json,
        .limit = if (join.right_filters) |filters| filters.limit else null,
    });
    defer params.deinit(alloc);

    const source_config = try foreign_source.toSourceConfig(alloc);
    var foreign_query_source = try registry.create(alloc, source_config);
    defer foreign_query_source.deinit(alloc);

    var result = try foreign_query_source.query(alloc, params);
    defer result.deinit(alloc);
    var hits = std.ArrayListUnmanaged(std.json.Value).empty;
    errdefer {
        for (hits.items) |*item| deinitJsonValue(alloc, item);
        hits.deinit(alloc);
    }

    for (result.rows) |row| {
        if (row != .object) return error.UnsupportedQueryRequest;
        const match_value = extractJsonPathValue(row, join.right_field) orelse continue;
        if (join.join_type != .right) {
            var matched = false;
            for (left_hits) |left_hit| {
                const left_value = extractJoinValueFromHit(left_hit, join.left_field) orelse continue;
                if (jsonValuesCompare(left_value, match_value, join.operator)) {
                    matched = true;
                    break;
                }
            }
            if (!matched) continue;
        }
        try hits.append(alloc, try buildForeignRightJoinHit(alloc, foreign_source, row, match_value));
    }

    const owned_slice = try hits.toOwnedSlice(alloc);
    if (join.nested_join) |nested_join| {
        try applyNestedJoinToRightHits(ctx, job_store, alloc, source, join.right_table, owned_slice, nested_join, foreign_sources);
    }
    return .{
        .owned_hits = owned_slice,
        .hits = owned_slice,
        .strategy_used = .broadcast,
    };
}

fn executeRightJoinBroadcastQueryLocal(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !RightJoinQueryResult {
    const nested_foreign_leaf_join = joinUsesNestedForeignLeafLocalFallback(join, foreign_sources);
    if (try executeNestedForeignLeafLookupFallback(ctx, job_store, alloc, source, join, left_hits, foreign_sources)) |result| {
        return result;
    }

    var query_value = try buildRightJoinQueryValue(alloc, join, left_hits);
    const attached_foreign_sources = !nested_foreign_leaf_join and join.nested_join != null and !foreign_sources.isEmpty();
    defer if (!attached_foreign_sources) deinitJsonValue(alloc, &query_value);
    if (nested_foreign_leaf_join and query_value == .object) {
        _ = query_value.object.orderedRemove("join");
    } else {
        try attachJoinForeignSourcesToQueryValue(alloc, &query_value, join, foreign_sources);
    }
    const query_body_for_log = try stringifyJsonValueAlloc(alloc, query_value);
    defer alloc.free(query_body_for_log);
    var loaded = loadRightJoinQueryAlloc(ctx, job_store, alloc, source, join, &query_value, nested_foreign_leaf_join, foreign_sources) catch |err| {
        std.log.err("right join broadcast query load failed table={s} err={} query={s}", .{
            join.right_table,
            err,
            query_body_for_log,
        });
        return err;
    };
    errdefer loaded.deinit();

    return .{
        .parsed = loaded.parsed,
        .hits = loaded.hits,
        .strategy_used = .broadcast,
    };
}

pub fn executeSupportedDistributedJoinPartitions(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    job_id: ?u64,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    appended_left_field: bool,
    plan: PlannedJoinExecution,
    resume_state: ?JoinShuffleResumeState,
) !?JoinPartitionExecutionResult {
    const engine: StatefulDistributedShuffleEngine = .{
        .ctx = ctx,
        .job_store = job_store,
        .alloc = alloc,
        .source = source,
        .join = join,
        .left_hits = left_hits,
        .appended_left_field = appended_left_field,
        .plan = plan,
    };
    return try engine.executePartitionsAlloc(job_id, resume_state);
}

pub fn executeGroupJoinPartitionRequest(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) ![]u8 {
    var result = try executeJoinPartitionWorkerLocal(ctx, job_store, alloc, source, group_id, table_name, body);
    defer result.deinit(alloc);
    return try job_store.encodeJoinPartitionResponse(alloc, result);
}

pub fn executeGroupJoinRowsRequest(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) ![]u8 {
    const hits = try executeJoinRowsLocal(ctx, alloc, source, group_id, table_name, body);
    defer {
        for (hits) |*hit| deinitJsonValue(alloc, hit);
        if (hits.len > 0) alloc.free(hits);
    }
    return try encodeJoinRowsResponse(alloc, hits);
}

pub fn executeGroupJoinUnmatchedRequest(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) ![]u8 {
    const response = try executeJoinUnmatchedLocal(ctx, alloc, source, group_id, table_name, body);
    defer {
        for (response.hits) |*hit| deinitJsonValue(alloc, @constCast(hit));
        if (response.hits.len > 0) alloc.free(response.hits);
    }
    return try encodeJoinUnmatchedResponse(alloc, response.hits, response.right_rows_scanned);
}

pub fn executeGroupJoinFinalizeRequest(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) ![]u8 {
    var result = try executeJoinFinalizeWorkerLocal(ctx, job_store, alloc, source, group_id, table_name, body);
    defer result.deinit(alloc);
    return try job_store.encodeJoinPartitionResponse(alloc, result);
}

pub fn executeGroupJoinJobStateRequest(
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]u8 {
    const job_id = try parseJoinJobStateRequest(alloc, body);
    return (try job_store.loadJoinJobStateSnapshot(alloc, job_id)) orelse error.UnknownGroup;
}

pub fn executeJoinFinalizeWorkerLocal(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    finalizer_group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !JoinPartitionExecutionResult {
    var req = try parseJoinFinalizeRequest(alloc, body);
    defer req.deinit(alloc);
    if (!std.mem.eql(u8, req.join.right_table, table_name)) return error.InvalidQueryRequest;
    const engine: StatefulDistributedShuffleEngine = .{
        .ctx = ctx,
        .job_store = job_store,
        .alloc = alloc,
        .source = source,
        .join = req.join,
        .left_hits = req.left_hits,
        .appended_left_field = req.appended_left_field,
        .plan = .{
            .strategy = .shuffle,
            .shuffle_partitions = req.shuffle_partitions,
        },
    };
    return try engine.executeFinalizerWorkerAlloc(
        finalizer_group_id,
        table_name,
        req.job_id,
        req.left_fields,
        req.handoff_owner_group_id,
    );
}

fn tryImportRemoteJoinJobState(
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    owner_group_id: u64,
    table_name: []const u8,
    job_id: u64,
) !bool {
    const body = try encodeJoinJobStateRequest(alloc, job_id);
    defer alloc.free(body);
    const response_value = try source.joinJobStateGroupLocal(alloc, owner_group_id, table_name, body) orelse return false;
    var response = response_value;
    defer response.deinit(alloc);
    try job_store.installJoinJobStateSnapshot(alloc, job_id, response.json);
    return true;
}

fn executeJoinRowsLocal(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) ![]std.json.Value {
    var req = try parseJoinRowsRequest(alloc, body);
    defer req.deinit(alloc);
    if (!std.mem.eql(u8, req.join.right_table, table_name)) return error.InvalidQueryRequest;
    if (req.join.nested_join != null) return error.UnsupportedQueryRequest;

    var query_value = try buildRightJoinPartitionRowsQueryValue(alloc, req.join);
    defer deinitJsonValue(alloc, &query_value);
    const query_body_for_log = try stringifyJsonValueAlloc(alloc, query_value);
    defer alloc.free(query_body_for_log);
    var owned_req = ctx.buildOwnedSearchRequest(alloc, req.join.right_table, query_value) catch |err| {
        std.log.err("join rows search request build failed group_id={d} table={s} err={} query={s}", .{
            group_id,
            req.join.right_table,
            err,
            query_body_for_log,
        });
        return err;
    };
    defer owned_req.deinit(alloc);

    var hits = std.json.Array.init(alloc);
    errdefer {
        for (hits.items) |*item| deinitJsonValue(alloc, item);
        hits.deinit();
    }
    if (!try appendGroupLocalJoinHits(alloc, source, group_id, req.join.right_table, owned_req.req, false, &hits)) return error.UnknownGroup;

    var partition_hits = std.json.Array.init(alloc);
    errdefer {
        for (partition_hits.items) |*item| deinitJsonValue(alloc, item);
        partition_hits.deinit();
    }
    for (hits.items) |hit| {
        const right_value = extractJoinValueFromHit(hit, req.join.right_field) orelse continue;
        if (partitionForJoinValue(right_value, req.partition_count) != req.partition_index) continue;
        try partition_hits.append(try cloneJsonValue(alloc, hit));
    }
    return try partition_hits.toOwnedSlice();
}

fn executeJoinUnmatchedLocal(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !EncodedJoinUnmatchedResponse {
    var req = try parseJoinUnmatchedRequest(alloc, body);
    defer req.deinit(alloc);
    if (!std.mem.eql(u8, req.join.right_table, table_name)) return error.InvalidQueryRequest;
    if (req.join.nested_join != null) return error.UnsupportedQueryRequest;

    var query_value = try buildRightJoinUnmatchedQueryValue(alloc, req.join, req.left_hit_count);
    defer deinitJsonValue(alloc, &query_value);
    var owned_req = try ctx.buildOwnedSearchRequest(alloc, req.join.right_table, query_value);
    defer owned_req.deinit(alloc);

    var matched_right_ids = try loadMatchedRightIdsAlloc(alloc, req.matched_right_ids);
    defer matched_right_ids.deinit(alloc);

    var hits = std.ArrayListUnmanaged(std.json.Value).empty;
    errdefer {
        for (hits.items) |*item| deinitJsonValue(alloc, item);
        hits.deinit(alloc);
    }

    const right_rows_scanned = (try appendGroupLocalUnmatchedRightJoinHits(
        alloc,
        source,
        group_id,
        req.join.right_table,
        owned_req.req,
        req.join,
        req.left_fields,
        req.appended_left_field,
        &matched_right_ids,
        &hits,
    )) orelse return error.UnknownGroup;

    return .{
        .hits = try hits.toOwnedSlice(alloc),
        .right_rows_scanned = @intCast(right_rows_scanned),
    };
}

fn buildRightJoinUnmatchedQueryValue(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
    left_hit_count: usize,
) !std.json.Value {
    std.debug.assert(join.join_type == .right);

    const filter_query_value = blk: {
        if (join.right_filters) |filters| {
            if (filters.filter_query) |filter_query| break :blk try cloneJsonValue(alloc, filter_query);
        }
        break :blk null;
    };

    var root = std.json.ObjectMap.empty;
    if (filter_query_value) |filter_query| {
        try root.put(alloc, try alloc.dupe(u8, "filter_query"), filter_query);
    } else {
        try root.put(alloc, try alloc.dupe(u8, "full_text_search"), try buildMatchAllQueryValue(alloc));
    }
    const requested_limit = if (join.right_filters) |filters| filters.limit else null;
    try root.put(alloc, try alloc.dupe(u8, "limit"), .{ .integer = @intCast(requested_limit orelse @max(@as(usize, 10), left_hit_count)) });
    if (join.right_filters) |filters| {
        if (filters.filter_prefix) |prefix| {
            try root.put(alloc, try alloc.dupe(u8, "filter_prefix"), .{ .string = try alloc.dupe(u8, prefix) });
        }
    }
    if (join.right_fields.len > 0 or !std.mem.eql(u8, join.right_field, "_id")) {
        var fields = std.json.Array.init(alloc);
        var saw_join_field = false;
        var saw_nested_left_field = false;
        for (join.right_fields) |field| {
            if (join.nested_join != null and std.mem.indexOfScalar(u8, field, '.') != null) continue;
            try fields.append(.{ .string = try alloc.dupe(u8, field) });
            if (std.mem.eql(u8, field, join.right_field)) saw_join_field = true;
            if (join.nested_join) |nested| {
                if (std.mem.eql(u8, field, nested.left_field)) saw_nested_left_field = true;
            }
        }
        if (!std.mem.eql(u8, join.right_field, "_id") and !saw_join_field) {
            try fields.append(.{ .string = try alloc.dupe(u8, join.right_field) });
        }
        if (join.nested_join) |nested| {
            if (!std.mem.eql(u8, nested.left_field, "_id") and !saw_nested_left_field) {
                try fields.append(.{ .string = try alloc.dupe(u8, nested.left_field) });
            }
        }
        try root.put(alloc, try alloc.dupe(u8, "fields"), .{ .array = fields });
    }
    return .{ .object = root };
}

pub fn executeJoinPartitionWorkerLocal(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    worker_group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !JoinPartitionExecutionResult {
    var req = parseJoinPartitionRequest(alloc, body) catch |err| {
        std.log.err("join partition request parse failed worker_group_id={d} table={s} err={} body={s}", .{
            worker_group_id,
            table_name,
            err,
            body,
        });
        return err;
    };
    defer req.deinit(alloc);
    if (!std.mem.eql(u8, req.join.right_table, table_name)) return error.InvalidQueryRequest;

    var right_hits_owned: ?[]std.json.Value = null;
    defer if (right_hits_owned) |owned| {
        for (owned) |*hit| deinitJsonValue(alloc, hit);
        alloc.free(owned);
    };
    const right_hits = if (req.right_group_ids.len > 0 and req.join.nested_join == null)
        collectJoinPartitionRightRows(ctx, job_store, alloc, source, worker_group_id, req) catch |err| {
            std.log.warn("join partition right-row collection failed worker_group_id={d} partition_index={d} err={}", .{
                worker_group_id,
                req.partition_index,
                err,
            });
            return err;
        }
    else blk: {
        var right_result = try executeRightJoinBroadcastQueryLocal(ctx, job_store, alloc, source, req.join, req.left_hits, .{});
        defer right_result.deinit(alloc);
        right_hits_owned = try alloc.alloc(std.json.Value, right_result.hits.len);
        for (right_result.hits, 0..) |hit, i| right_hits_owned.?[i] = try cloneJsonValue(alloc, hit);
        break :blk right_hits_owned.?;
    };

    var merged = mergeJoinedRightHitsAlloc(alloc, req.left_hits, req.join, right_hits, &.{}, req.appended_left_field) catch |err| {
        std.log.err("join partition merge failed worker_group_id={d} partition_index={d} err={}", .{
            worker_group_id,
            req.partition_index,
            err,
        });
        return err;
    };
    errdefer merged.deinit(alloc);
    return joinPartitionExecutionResultFromShell(merged, .{});
}

fn collectJoinPartitionRightRows(
    ctx: JoinContext,
    _: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    worker_group_id: u64,
    req: JoinPartitionRequest,
) ![]std.json.Value {
    var out = std.json.Array.init(alloc);
    errdefer {
        for (out.items) |*item| deinitJsonValue(alloc, item);
        out.deinit();
    }

    const body = try encodeJoinRowsRequest(alloc, req.job_id, req.join, req.partition_index, req.partition_count);
    defer alloc.free(body);

    for (req.right_group_ids) |target_group_id| {
        if (target_group_id == worker_group_id) {
            const local_hits = try executeJoinRowsLocal(ctx, alloc, source, target_group_id, req.join.right_table, body);
            defer {
                for (local_hits) |*hit| deinitJsonValue(alloc, hit);
                if (local_hits.len > 0) alloc.free(local_hits);
            }
            try appendClonedJsonHitsToArray(alloc, &out, local_hits);
            continue;
        }

        if (try source.joinRowsGroupLocal(alloc, target_group_id, req.join.right_table, body)) |response_value| {
            var response = response_value;
            defer response.deinit(alloc);
            const remote_hits = try parseJoinRowsResponse(alloc, response.json);
            defer {
                for (remote_hits) |*hit| deinitJsonValue(alloc, hit);
                if (remote_hits.len > 0) alloc.free(remote_hits);
            }
            try appendClonedJsonHitsToArray(alloc, &out, remote_hits);
            continue;
        }

        const local_fallback_hits = try executeJoinRowsLocal(ctx, alloc, source, target_group_id, req.join.right_table, body);
        defer {
            for (local_fallback_hits) |*hit| deinitJsonValue(alloc, hit);
            if (local_fallback_hits.len > 0) alloc.free(local_fallback_hits);
        }
        try appendClonedJsonHitsToArray(alloc, &out, local_fallback_hits);
    }

    return try out.toOwnedSlice();
}

fn dispatchJoinPartitionToWorker(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    job_id: ?u64,
    worker_group_id: u64,
    right_group_ids: []const u64,
    partition_index: usize,
    partition_count: usize,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    appended_left_field: bool,
) !?JoinPartitionExecutionResult {
    const body = try encodeJoinPartitionRequest(alloc, job_id, join, left_hits, appended_left_field, partition_index, partition_count, right_group_ids);
    defer alloc.free(body);

    const partition_response_opt = try source.joinPartitionGroupLocal(alloc, worker_group_id, join.right_table, body);
    if (partition_response_opt) |response_value| {
        var response = response_value;
        defer response.deinit(alloc);
        return job_store.parseJoinPartitionResponse(alloc, response.json) catch |err| {
            std.log.err("distributed join partition response parse failed worker_group_id={d} err={} body={s}", .{
                worker_group_id,
                err,
                response.json,
            });
            return err;
        };
    }

    return try executeJoinPartitionWorkerLocal(ctx, job_store, alloc, source, worker_group_id, join.right_table, body);
}

fn appendDistributedRightJoinUnmatchedRows(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    left_fields: []const []const u8,
    appended_left_field: bool,
    distributed_join: *JoinPartitionExecutionResult,
) !void {
    const effective_matched_ids = try rightJoinMatchedIdsForUnmatchedAlloc(alloc, join, distributed_join);
    defer {
        for (effective_matched_ids) |id| alloc.free(id);
        if (effective_matched_ids.len > 0) alloc.free(effective_matched_ids);
    }
    if (try buildDistributedRightJoinUnmatchedCompletionAcrossGroupsAlloc(
        ctx,
        alloc,
        source,
        join,
        left_hits,
        left_fields,
        appended_left_field,
        effective_matched_ids,
    )) |completion_value| {
        var completion = completion_value;
        defer completion.deinit(alloc);
        try appendBuiltDistributedRightJoinUnmatchedHitsToResultAlloc(alloc, distributed_join, &completion);
        return;
    }

    var candidates = try loadDistributedRightJoinUnmatchedCandidatesAlloc(
        ctx,
        job_store,
        alloc,
        source,
        join,
        left_hits,
        effective_matched_ids,
    );
    defer candidates.deinit(alloc);

    const appended_hits = try buildDistributedRightJoinUnmatchedHitsAlloc(
        alloc,
        candidates.right_result.hits,
        join,
        left_fields,
        appended_left_field,
        &candidates.matched_right_ids,
    );
    errdefer {
        for (appended_hits) |*item| deinitJsonValue(alloc, item);
        if (appended_hits.len > 0) alloc.free(appended_hits);
    }
    var completion: DistributedRightJoinUnmatchedCompletion = .{
        .hits = appended_hits,
        .groups_queried = candidates.right_result.groups_queried,
        .right_rows_scanned = candidates.right_result.hits.len,
    };
    errdefer completion.deinit(alloc);
    try appendBuiltDistributedRightJoinUnmatchedHitsToResultAlloc(alloc, distributed_join, &completion);
}

fn rightJoinMatchedIdsForUnmatchedAlloc(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
    distributed_join: *const JoinPartitionExecutionResult,
) ![][]u8 {
    var ids = std.StringHashMapUnmanaged(void){};
    defer ids.deinit(alloc);
    for (distributed_join.matched_right_ids) |matched_id| {
        try ids.put(alloc, matched_id, {});
    }
    for (distributed_join.hits) |hit| {
        const left_value = extractJoinValueFromHit(hit, join.left_field) orelse continue;
        const text = switch (left_value) {
            .string => |value| value,
            else => continue,
        };
        try ids.put(alloc, text, {});
    }

    const out = try alloc.alloc([]u8, ids.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |id| alloc.free(id);
        if (out.len > 0) alloc.free(out);
    }
    var iter = ids.iterator();
    while (iter.next()) |entry| {
        out[initialized] = try alloc.dupe(u8, entry.key_ptr.*);
        initialized += 1;
    }
    return out;
}

fn buildDistributedRightJoinUnmatchedCompletionAcrossGroupsAlloc(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    left_fields: []const []const u8,
    appended_left_field: bool,
    matched_ids: []const []const u8,
) !?DistributedRightJoinUnmatchedCompletion {
    if (join.nested_join != null) return null;
    const explicit_right_limit = join.right_filters != null and join.right_filters.?.limit != null;
    if (explicit_right_limit) return null;

    var groups = (try DistributedRightJoinGroups.init(ctx, alloc, join.right_table, 2)) orelse return null;
    defer groups.deinit();

    const body = try encodeJoinUnmatchedRequest(alloc, join, left_hits.len, left_fields, appended_left_field, matched_ids);
    defer alloc.free(body);

    var appended_hits = std.ArrayListUnmanaged(std.json.Value).empty;
    var completed = false;
    defer if (!completed) {
        for (appended_hits.items) |*item| deinitJsonValue(alloc, item);
        appended_hits.deinit(alloc);
    };

    var groups_queried: usize = 0;
    var right_rows_scanned: usize = 0;
    for (groups.group_ids) |group_id| {
        const response_value = if (try source.joinUnmatchedGroupLocal(alloc, group_id, join.right_table, body)) |response|
            response
        else blk: {
            const local_body = executeGroupJoinUnmatchedRequest(ctx, alloc, source, group_id, join.right_table, body) catch |err| switch (err) {
                error.UnknownGroup => return null,
                else => return err,
            };
            break :blk query_api.QueryResponse{ .json = local_body };
        };
        var response = response_value;
        defer response.deinit(alloc);
        var group_result = try parseJoinUnmatchedResponse(alloc, response.json);
        defer {
            for (group_result.hits) |*item| deinitJsonValue(alloc, @constCast(item));
            if (group_result.hits.len > 0) alloc.free(group_result.hits);
        }
        for (group_result.hits) |hit| try appended_hits.append(alloc, hit);
        if (group_result.hits.len > 0) alloc.free(group_result.hits);
        group_result.hits = &.{};
        groups_queried += 1;
        right_rows_scanned += @intCast(group_result.right_rows_scanned);
    }

    const owned_hits = try appended_hits.toOwnedSlice(alloc);
    completed = true;
    return .{
        .hits = owned_hits,
        .groups_queried = groups_queried,
        .right_rows_scanned = right_rows_scanned,
    };
}

fn appendGroupLocalUnmatchedRightJoinHits(
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    join: SupportedJoinRequest,
    left_fields: []const []const u8,
    appended_left_field: bool,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
    out: *std.ArrayListUnmanaged(std.json.Value),
) !?usize {
    var query_req = req;
    query_req.offset = 0;
    query_req.limit = unmatched_right_join_group_chunk_limit;

    var total_hits: ?usize = null;
    var scanned_hits: usize = 0;
    while (true) {
        if (try source.searchResultGroupLocal(alloc, group_id, table_name, query_req, .read_index)) |search_result_value| {
            var search_result = search_result_value;
            defer search_result.deinit();
            if (total_hits == null) total_hits = search_result.total_hits;
            if (search_result.hits.len == 0) break;

            _ = try join_model.appendUnmatchedRightJoinSearchHitsAlloc(
                alloc,
                out,
                search_result.hits,
                join,
                left_fields,
                appended_left_field,
                matched_right_ids,
            );

            scanned_hits += search_result.hits.len;
            if (scanned_hits >= search_result.total_hits) break;
            query_req.offset = @intCast(scanned_hits);
            continue;
        }

        var response = (try source.queryGroupLocal(alloc, group_id, table_name, query_req, .read_index)) orelse return null;
        defer response.deinit(alloc);

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, response.json, .{}) catch return error.InternalFailure;
        defer parsed.deinit();
        const page_total_hits = try queryTotalHits(parsed.value);
        if (total_hits == null) total_hits = page_total_hits;
        const hits_ptr = try queryHitsArrayPtr(&parsed.value);
        if (hits_ptr.items.len == 0) break;

        _ = try join_model.appendUnmatchedRightJoinHitsAlloc(
            alloc,
            out,
            hits_ptr.items,
            join,
            left_fields,
            appended_left_field,
            matched_right_ids,
        );

        scanned_hits += hits_ptr.items.len;
        if (scanned_hits >= page_total_hits) break;
        query_req.offset = @intCast(scanned_hits);
    }

    return total_hits orelse 0;
}

fn loadDistributedRightJoinUnmatchedCandidatesAlloc(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    matched_ids: []const []const u8,
) !DistributedRightJoinUnmatchedCandidates {
    var right_result = try executeRightJoinDistributedBroadcastQuery(ctx, alloc, source, join, left_hits) orelse
        try executeRightJoinBroadcastQueryLocal(ctx, job_store, alloc, source, join, left_hits, .{});
    errdefer right_result.deinit(alloc);

    const matched_right_ids = try loadMatchedRightIdsAlloc(alloc, matched_ids);

    return .{
        .right_result = right_result,
        .matched_right_ids = matched_right_ids,
    };
}

fn loadMatchedRightIdsAlloc(
    alloc: std.mem.Allocator,
    matched_ids: []const []const u8,
) !std.StringHashMapUnmanaged(void) {
    var matched_right_ids = std.StringHashMapUnmanaged(void){};
    errdefer matched_right_ids.deinit(alloc);
    for (matched_ids) |matched_id| {
        try matched_right_ids.put(alloc, matched_id, {});
    }
    return matched_right_ids;
}

fn buildDistributedRightJoinUnmatchedHitsAlloc(
    alloc: std.mem.Allocator,
    right_hits: []const std.json.Value,
    join: SupportedJoinRequest,
    left_fields: []const []const u8,
    appended_left_field: bool,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
) ![]std.json.Value {
    var appended_hits = std.ArrayListUnmanaged(std.json.Value).empty;
    errdefer {
        for (appended_hits.items) |*item| deinitJsonValue(alloc, item);
        appended_hits.deinit(alloc);
    }

    _ = try join_model.appendUnmatchedRightJoinHitsAlloc(
        alloc,
        &appended_hits,
        right_hits,
        join,
        left_fields,
        appended_left_field,
        matched_right_ids,
    );
    return try appended_hits.toOwnedSlice(alloc);
}

fn requestedFieldNamesAlloc(
    alloc: std.mem.Allocator,
    left_fields: []const std.json.Value,
) ![]const []const u8 {
    const out = try alloc.alloc([]const u8, left_fields.len);
    errdefer alloc.free(out);
    for (left_fields, 0..) |field, i| {
        out[i] = switch (field) {
            .string => |value| value,
            else => return error.InvalidQueryRequest,
        };
    }
    return out;
}

fn appendBuiltDistributedRightJoinUnmatchedHitsToResultAlloc(
    alloc: std.mem.Allocator,
    distributed_join: *JoinPartitionExecutionResult,
    completion: *DistributedRightJoinUnmatchedCompletion,
) !void {
    const existing_hits = distributed_join.hits;
    if (completion.hits.len == 0) return;

    const merged_len = existing_hits.len + completion.hits.len;
    const merged_hits = try alloc.alloc(std.json.Value, merged_len);
    errdefer alloc.free(merged_hits);
    for (existing_hits, 0..) |hit, i| {
        merged_hits[i] = hit;
    }
    for (completion.hits, 0..) |hit, i| {
        merged_hits[existing_hits.len + i] = hit;
    }
    if (existing_hits.len > 0) alloc.free(existing_hits);
    alloc.free(completion.hits);
    distributed_join.hits = merged_hits;
    distributed_join.stats.right_rows_scanned += @intCast(completion.right_rows_scanned);
    distributed_join.stats.rows_unmatched_right += @intCast(completion.hits.len);
    distributed_join.groups_queried = @max(distributed_join.groups_queried, completion.groups_queried);
    completion.hits = &.{};
    completion.groups_queried = 0;
    completion.right_rows_scanned = 0;
}

fn executeRightJoinLookupQuery(
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
) !RightJoinQueryResult {
    var seen = std.StringHashMapUnmanaged(void){};
    defer seen.deinit(alloc);

    var hits = std.json.Array.init(alloc);
    errdefer {
        for (hits.items) |*item| deinitJsonValue(alloc, item);
        hits.deinit();
    }

    for (left_hits) |hit_value| {
        const left_value = extractJoinValueFromHit(hit_value, join.left_field) orelse continue;
        if (left_value != .string) return error.UnsupportedQueryRequest;
        if (seen.contains(left_value.string)) continue;
        try seen.put(alloc, left_value.string, {});
        var lookup = (try source.lookup(alloc, join.right_table, left_value.string, .{}, .read_index)) orelse continue;
        defer lookup.deinit(alloc);

        var stored = std.json.parseFromSlice(std.json.Value, alloc, lookup.json, .{}) catch return error.InternalFailure;
        defer stored.deinit();

        var hit_obj = std.json.ObjectMap.empty;
        try hit_obj.put(alloc, try alloc.dupe(u8, "_id"), .{ .string = try alloc.dupe(u8, left_value.string) });
        try hit_obj.put(alloc, try alloc.dupe(u8, "_score"), .{ .float = 0 });
        try hit_obj.put(alloc, try alloc.dupe(u8, "_source"), try cloneJsonValue(alloc, stored.value));
        try hits.append(.{ .object = hit_obj });
    }

    const owned_slice = try hits.toOwnedSlice();
    return .{
        .owned_hits = owned_slice,
        .hits = owned_slice,
        .strategy_used = .index_lookup,
    };
}

pub fn planSupportedJoinExecution(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    left_table_name: []const u8,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !PlannedJoinExecution {
    const left_rows: u64 = @intCast(left_hits.len);
    const distinct_left_keys = join_model.countDistinctJoinKeys(alloc, left_hits, join.left_field, extractJoinValueFromHit) catch 0;
    const supported_index_lookup = join_model.joinSupportsIndexLookup(join);

    var plan: PlannedJoinExecution = .{
        .estimated_rows = left_rows,
    };

    const foreign_right_stats = if (foreign_sources.get(join.right_table)) |foreign_source|
        estimateForeignJoinTableStats(ctx, alloc, foreign_source)
    else
        null;

    var snapshot = (try ctx.adminSnapshot()) orelse {
        const right_stats = foreign_right_stats orelse JoinTableStats{};
        plan.used_stats = right_stats.has_stats;
        if (plan.used_stats) {
            chooseJoinExecutionStrategyWithStats(&plan, join, supported_index_lookup, left_rows, distinct_left_keys, .{}, right_stats);
        } else {
            plan.strategy = join_model.chooseJoinExecutionStrategyWithoutStats(RightJoinQueryResult.StrategyUsed, join, supported_index_lookup);
        }
        estimateJoinPlanCosts(&plan, plan.strategy, left_rows, right_stats.row_count, right_stats.size_bytes);
        return plan;
    };
    defer ctx.freeAdminSnapshot(&snapshot);

    const left_stats = estimateJoinTableStatsFromSnapshot(&snapshot, left_table_name);
    const right_stats = foreign_right_stats orelse estimateJoinTableStatsFromSnapshot(&snapshot, join.right_table);
    plan.used_stats = left_stats.has_stats or right_stats.has_stats;

    if (join_model.resolveJoinStrategyHint(RightJoinQueryResult.StrategyUsed, join, supported_index_lookup)) |hint| {
        if (hint.shuffle_requested) {
            chooseStatefulShuffleStrategyOrForcedBroadcast(join, left_rows, right_stats.size_bytes).apply(&plan);
        } else {
            (StatefulJoinStrategyDecision{
                .strategy = hint.strategy,
                .forced_broadcast_fallback = hint.forced_broadcast_fallback,
            }).apply(&plan);
        }
        estimateJoinPlanCosts(&plan, plan.strategy, left_rows, right_stats.row_count, right_stats.size_bytes);
        return plan;
    }

    if (!plan.used_stats) {
        plan.strategy = join_model.chooseJoinExecutionStrategyWithoutStats(RightJoinQueryResult.StrategyUsed, join, supported_index_lookup);
        estimateJoinPlanCosts(&plan, plan.strategy, left_rows, right_stats.row_count, right_stats.size_bytes);
        return plan;
    }

    chooseJoinExecutionStrategyWithStats(&plan, join, supported_index_lookup, left_rows, distinct_left_keys, left_stats, right_stats);

    estimateJoinPlanCosts(&plan, plan.strategy, left_rows, right_stats.row_count, right_stats.size_bytes);
    return plan;
}

fn executeRightJoinDistributedLookupQuery(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
) !?RightJoinQueryResult {
    if (join.right_filters != null or join.nested_join != null or !std.mem.eql(u8, join.right_field, "_id")) return null;

    var groups = (try DistributedRightJoinGroups.init(ctx, alloc, join.right_table, 1)) orelse return null;
    defer groups.deinit();

    var owned_hits = std.json.Array.init(alloc);
    errdefer {
        for (owned_hits.items) |*item| deinitJsonValue(alloc, item);
        owned_hits.deinit();
    }

    var groups_queried: usize = 0;
    var saw_unresolved_key = false;
    for (groups.group_ids) |group_id| {
        var partition_left_hits = std.json.Array.init(alloc);
        defer partition_left_hits.deinit();
        for (left_hits) |hit| {
            const left_value = extractJoinValueFromHit(hit, join.left_field) orelse continue;
            if (left_value != .string) {
                saw_unresolved_key = true;
                continue;
            }
            const resolved_group_id = rightJoinGroupForKey(&groups.snapshot, groups.table_id, left_value.string) orelse {
                saw_unresolved_key = true;
                continue;
            };
            if (resolved_group_id != group_id) continue;
            try partition_left_hits.append(hit);
        }
        if (partition_left_hits.items.len == 0) continue;
        groups_queried += 1;
        var query_value = try buildRightJoinQueryValue(alloc, join, partition_left_hits.items);
        defer deinitJsonValue(alloc, &query_value);
        var owned_req = try ctx.buildOwnedSearchRequest(alloc, join.right_table, query_value);
        defer owned_req.deinit(alloc);
        if (!try appendGroupLocalJoinHits(alloc, source, group_id, join.right_table, owned_req.req, false, &owned_hits)) return null;
    }

    if (groups_queried == 0 or saw_unresolved_key) return null;
    const owned_slice = try owned_hits.toOwnedSlice();
    return .{
        .owned_hits = owned_slice,
        .hits = owned_slice,
        .strategy_used = .index_lookup,
        .distributed_execution = true,
        .groups_queried = groups_queried,
    };
}

fn executeRightJoinDistributedBroadcastQuery(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
) !?RightJoinQueryResult {
    if (join.nested_join != null) return null;
    const explicit_right_limit = join.right_filters != null and join.right_filters.?.limit != null;
    if (explicit_right_limit) return null;

    var groups = (try DistributedRightJoinGroups.init(ctx, alloc, join.right_table, 2)) orelse return null;
    defer groups.deinit();

    var query_value = try buildRightJoinQueryValue(alloc, join, left_hits);
    defer deinitJsonValue(alloc, &query_value);
    var owned_req = try ctx.buildOwnedSearchRequest(alloc, join.right_table, query_value);
    defer owned_req.deinit(alloc);

    var owned_hits = std.json.Array.init(alloc);
    errdefer {
        for (owned_hits.items) |*item| deinitJsonValue(alloc, item);
        owned_hits.deinit();
    }

    const groups_queried = try appendJoinHitsAcrossGroups(
        alloc,
        source,
        groups.group_ids,
        join.right_table,
        owned_req.req,
        &owned_hits,
    ) orelse return null;

    const owned_slice = try owned_hits.toOwnedSlice();
    return .{
        .owned_hits = owned_slice,
        .hits = owned_slice,
        .strategy_used = .broadcast,
        .distributed_execution = true,
        .groups_queried = groups_queried,
    };
}

const LocalShufflePartitionContext = struct {
    ctx: JoinContext,
    job_store: *JoinJobStore,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    foreign_sources: foreign_mod.PostgresSourceMap,
};

const DistributedShufflePartitionContext = struct {
    ctx: JoinContext,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    group_ids: []const u64,
};

fn appendRightJoinShufflePartitions(
    alloc: std.mem.Allocator,
    left_hits: []const std.json.Value,
    left_field: []const u8,
    partition_count: usize,
    partition_context: anytype,
    hits: *std.json.Array,
    comptime append_partition_hits: fn (@TypeOf(partition_context), std.mem.Allocator, []const std.json.Value, *std.json.Array) anyerror!?usize,
) !?usize {
    var accumulated: usize = 0;
    for (0..partition_count) |partition_index| {
        const partition_left_hits = try collectLeftHitsForJoinPartitionAlloc(
            alloc,
            left_hits,
            left_field,
            partition_index,
            partition_count,
        );
        defer if (partition_left_hits.len > 0) alloc.free(partition_left_hits);
        if (partition_left_hits.len == 0) continue;
        accumulated += (try append_partition_hits(partition_context, alloc, partition_left_hits, hits)) orelse return null;
    }
    return accumulated;
}

fn appendLocalShufflePartitionHits(
    partition_context: LocalShufflePartitionContext,
    alloc: std.mem.Allocator,
    partition_left_hits: []const std.json.Value,
    hits: *std.json.Array,
) !?usize {
    const nested_foreign_leaf_join = joinUsesNestedForeignLeafLocalFallback(partition_context.join, partition_context.foreign_sources);
    if (try executeNestedForeignLeafLookupFallback(
        partition_context.ctx,
        partition_context.job_store,
        alloc,
        partition_context.source,
        partition_context.join,
        partition_left_hits,
        partition_context.foreign_sources,
    )) |base_result_val| {
        var base_result = base_result_val;
        defer base_result.deinit(alloc);
        try appendClonedJsonHitsToArray(alloc, hits, base_result.hits);
        return 0;
    }

    var query_value = try buildRightJoinQueryValue(alloc, partition_context.join, partition_left_hits);
    const attached_foreign_sources = !nested_foreign_leaf_join and partition_context.join.nested_join != null and !partition_context.foreign_sources.isEmpty();
    defer if (!attached_foreign_sources) deinitJsonValue(alloc, &query_value);
    if (nested_foreign_leaf_join and std.meta.activeTag(query_value) == .object) {
        _ = query_value.object.orderedRemove("join");
    } else {
        try attachJoinForeignSourcesToQueryValue(alloc, &query_value, partition_context.join, partition_context.foreign_sources);
    }
    var loaded = try loadRightJoinQueryAlloc(
        partition_context.ctx,
        partition_context.job_store,
        alloc,
        partition_context.source,
        partition_context.join,
        &query_value,
        nested_foreign_leaf_join,
        partition_context.foreign_sources,
    );
    defer loaded.deinit();
    try appendClonedJsonHitsToArray(alloc, hits, loaded.hits);
    return 0;
}

fn appendDistributedShufflePartitionHits(
    partition_context: DistributedShufflePartitionContext,
    alloc: std.mem.Allocator,
    partition_left_hits: []const std.json.Value,
    hits: *std.json.Array,
) !?usize {
    var query_value = try buildRightJoinQueryValue(alloc, partition_context.join, partition_left_hits);
    defer deinitJsonValue(alloc, &query_value);
    var owned_req = try partition_context.ctx.buildOwnedSearchRequest(alloc, partition_context.join.right_table, query_value);
    defer owned_req.deinit(alloc);
    return try appendJoinHitsAcrossGroups(
        alloc,
        partition_context.source,
        partition_context.group_ids,
        partition_context.join.right_table,
        owned_req.req,
        hits,
    );
}

fn executeRightJoinShuffleQuery(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    plan: PlannedJoinExecution,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !RightJoinQueryResult {
    const partition_count = @max(@as(usize, 1), plan.shuffle_partitions);
    var owned_hits = std.json.Array.init(alloc);
    errdefer {
        for (owned_hits.items) |*item| deinitJsonValue(alloc, item);
        owned_hits.deinit();
    }

    _ = (try appendRightJoinShufflePartitions(
        alloc,
        left_hits,
        join.left_field,
        partition_count,
        LocalShufflePartitionContext{
            .ctx = ctx,
            .job_store = job_store,
            .source = source,
            .join = join,
            .foreign_sources = foreign_sources,
        },
        &owned_hits,
        appendLocalShufflePartitionHits,
    )).?;

    const owned_slice = try owned_hits.toOwnedSlice();
    return .{
        .owned_hits = owned_slice,
        .hits = owned_slice,
        .strategy_used = .shuffle,
    };
}

fn executeRightJoinDistributedShuffleQuery(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    plan: PlannedJoinExecution,
) !?RightJoinQueryResult {
    if (join.nested_join != null) return null;
    const explicit_right_limit = join.right_filters != null and join.right_filters.?.limit != null;
    if (explicit_right_limit) return null;

    var groups = (try DistributedRightJoinGroups.init(ctx, alloc, join.right_table, 2)) orelse return null;
    defer groups.deinit();

    const partition_count = @max(@as(usize, 1), plan.shuffle_partitions);
    var owned_hits = std.json.Array.init(alloc);
    errdefer {
        for (owned_hits.items) |*item| deinitJsonValue(alloc, item);
        owned_hits.deinit();
    }

    const groups_queried = (try appendRightJoinShufflePartitions(
        alloc,
        left_hits,
        join.left_field,
        partition_count,
        DistributedShufflePartitionContext{
            .ctx = ctx,
            .source = source,
            .join = join,
            .group_ids = groups.group_ids,
        },
        &owned_hits,
        appendDistributedShufflePartitionHits,
    )) orelse return null;

    const owned_slice = try owned_hits.toOwnedSlice();
    return .{
        .owned_hits = owned_slice,
        .hits = owned_slice,
        .strategy_used = .shuffle,
        .distributed_execution = true,
        .groups_queried = groups_queried,
    };
}

fn executeNestedForeignLeafLookupFallback(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !?RightJoinQueryResult {
    if (!joinUsesNestedForeignLeafLocalFallback(join, foreign_sources)) return null;
    if (join.right_filters != null or !std.mem.eql(u8, join.right_field, "_id")) return null;

    var base_join = join;
    base_join.nested_join = null;
    var result = executeRightJoinLookupQuery(alloc, source, base_join, left_hits) catch |err| {
        std.log.err("nested foreign leaf lookup failed table={s} err={}", .{ join.right_table, err });
        return err;
    };
    errdefer result.deinit(alloc);
    applyNestedJoinToRightHits(ctx, job_store, alloc, source, join.right_table, result.owned_hits, join.nested_join.?, foreign_sources) catch |err| {
        std.log.err("nested foreign leaf join fallback failed table={s} nested={s} err={}", .{ join.right_table, join.nested_join.?.right_table, err });
        return err;
    };
    return result;
}

fn loadRightJoinQueryAlloc(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    query_value: *std.json.Value,
    nested_foreign_leaf_join: bool,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !LoadedRightJoinQuery {
    var parsed = try executeRightJoinQueryParsedAlloc(ctx, alloc, source, join, query_value.*, nested_foreign_leaf_join);
    errdefer parsed.deinit();
    const total_hits = try queryTotalHits(parsed.value);
    var hits_ptr = try queryHitsArrayPtr(&parsed.value);
    const explicit_right_limit = join.right_filters != null and join.right_filters.?.limit != null;

    if (!explicit_right_limit and hits_ptr.items.len < total_hits) {
        if (query_value.* != .object) return error.InternalFailure;
        try putOwnedJsonField(alloc, &query_value.object, "limit", .{ .integer = @intCast(total_hits) });
        parsed.deinit();
        parsed = try executeRightJoinQueryParsedAlloc(ctx, alloc, source, join, query_value.*, nested_foreign_leaf_join);
        hits_ptr = try queryHitsArrayPtr(&parsed.value);
    }

    if (nested_foreign_leaf_join) {
        try applyNestedJoinToRightHits(ctx, job_store, alloc, source, join.right_table, hits_ptr.items, join.nested_join.?, foreign_sources);
    }

    return .{
        .parsed = parsed,
        .hits = hits_ptr.items,
    };
}

fn executeRightJoinQueryParsedAlloc(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    join: SupportedJoinRequest,
    query_value: std.json.Value,
    nested_foreign_leaf_join: bool,
) !std.json.Parsed(std.json.Value) {
    const query_body = try stringifyJsonValueAlloc(alloc, query_value);
    defer alloc.free(query_body);

    const right_json = if (join.nested_join != null and !nested_foreign_leaf_join)
        ctx.executeQueryDispatch(alloc, source, join.right_table, query_body, null) catch |err| {
            std.log.err("right join dispatch query failed table={s} err={} body={s}", .{
                join.right_table,
                err,
                query_body,
            });
            return err;
        }
    else blk: {
        var right_result = ctx.executePlainQuery(alloc, source, join.right_table, query_body, null) catch |err| {
            std.log.err("right join plain query failed table={s} err={} body={s}", .{
                join.right_table,
                err,
                query_body,
            });
            return err;
        };
        defer right_result.deinit(alloc);
        break :blk try alloc.dupe(u8, right_result.json);
    };
    defer alloc.free(right_json);

    return std.json.parseFromSlice(std.json.Value, alloc, right_json, .{}) catch return error.InternalFailure;
}

fn applyNestedJoinToRightHits(
    ctx: JoinContext,
    job_store: *JoinJobStore,
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    parent_table_name: []const u8,
    right_hits: []std.json.Value,
    nested_join: *SupportedJoinRequest,
    foreign_sources: foreign_mod.PostgresSourceMap,
) anyerror!void {
    if (right_hits.len == 0) return;
    const plan = try planSupportedJoinExecution(ctx, alloc, parent_table_name, nested_join.*, right_hits, foreign_sources);
    var nested_result = try executeSupportedRightJoinQuery(ctx, job_store, alloc, source, nested_join.*, right_hits, plan, foreign_sources);
    defer nested_result.deinit(alloc);

    for (right_hits) |*hit| {
        const left_value = extractJoinValueFromHit(hit.*, nested_join.left_field) orelse continue;
        const matched_right = findFirstMatchingRightHit(nested_join.*, left_value, nested_result.hits) orelse continue;
        const source_value = hit.object.getPtr("_source") orelse return error.InvalidQueryRequest;
        if (source_value.* != .object) return error.InvalidQueryRequest;
        try mergeRightHitIntoSource(alloc, source_value, nested_join.*, matched_right);
    }
}

fn estimateForeignJoinTableStats(
    ctx: JoinContext,
    alloc: std.mem.Allocator,
    foreign_source: foreign_mod.PostgresConfig,
) ?JoinTableStats {
    const registry = ctx.ensureForeignRegistry() catch return null;
    const source_config = foreign_source.toSourceConfig(alloc) catch return null;

    var source = registry.create(alloc, source_config) catch return null;
    defer source.deinit(alloc);

    const stats = source.statistics(foreign_source.postgres_table) catch return null;
    return .{
        .row_count = @intCast(@max(stats.row_count, 0)),
        .size_bytes = @intCast(@max(stats.size_bytes, 0)),
        .has_stats = true,
    };
}

// ---------------------------------------------------------------------------
// Pure utility functions (no self/ctx/job_store)
// ---------------------------------------------------------------------------

pub fn freeSupportedJoinRequest(alloc: std.mem.Allocator, join: *const SupportedJoinRequest) void {
    var owned = join.*;
    owned.deinit(alloc);
}

pub fn joinUsesForeignSource(join: SupportedJoinRequest, foreign_sources: foreign_mod.PostgresSourceMap) bool {
    if (foreign_sources.contains(join.right_table)) return true;
    if (join.nested_join) |nested| return joinUsesForeignSource(nested.*, foreign_sources);
    return false;
}

pub fn parseSupportedJoinRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !?ParsedSupportedJoinRequest {
    return try parseSupportedJoinRequestWithSecrets(alloc, body, null);
}

pub fn parseSupportedJoinRequestWithSecrets(
    alloc: std.mem.Allocator,
    body: []const u8,
    secret_store: ?*@import("../common/secrets.zig").FileStore,
) !?ParsedSupportedJoinRequest {
    var parsed_request = metadata_openapi.server.parseQueryTableBody(alloc, body) catch return error.InvalidQueryRequest;
    defer parsed_request.deinit();
    const join = parsed_request.value.join orelse return null;
    return .{
        .join = try supportedJoinRequestFromOpenApi(alloc, join),
        .foreign_sources = foreign_sources_api.postgresSourceMapFromMetadataOpenApiResolvedWithSecrets(alloc, parsed_request.value.foreign_sources, secret_store) catch |err| switch (err) {
            error.UnsupportedSourceKind => return error.UnsupportedQueryRequest,
            else => return err,
        },
    };
}

pub fn parseSupportedJoinClauseValue(
    alloc: std.mem.Allocator,
    join_value: std.json.Value,
) anyerror!SupportedJoinRequest {
    const encoded = try stringifyJsonValueAlloc(alloc, join_value);
    defer alloc.free(encoded);
    var parsed = std.json.parseFromSlice(metadata_openapi.JoinClause, alloc, encoded, .{
        .ignore_unknown_fields = true,
    }) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    return try supportedJoinRequestFromOpenApi(alloc, parsed.value);
}

pub fn supportedJoinRequestFromOpenApi(
    alloc: std.mem.Allocator,
    join: metadata_openapi.JoinClause,
) !SupportedJoinRequest {
    if (join.right_table.len == 0 or join.on.left_field.len == 0 or join.on.right_field.len == 0) {
        return error.InvalidQueryRequest;
    }
    const right_table = try alloc.dupe(u8, join.right_table);
    errdefer alloc.free(right_table);
    const left_field = try alloc.dupe(u8, join.on.left_field);
    errdefer alloc.free(left_field);
    const right_field = try alloc.dupe(u8, join.on.right_field);
    errdefer alloc.free(right_field);

    var right_filters = try supportedJoinFiltersFromOpenApi(alloc, join.right_filters);
    errdefer if (right_filters) |*filters| filters.deinit(alloc);
    const right_fields = try cloneFieldList(alloc, join.right_fields orelse &.{});
    errdefer freeFieldList(alloc, right_fields);
    const strategy_hint = if (join.strategy_hint) |hint|
        try alloc.dupe(u8, @tagName(hint))
    else
        null;
    errdefer if (strategy_hint) |value| alloc.free(value);

    var nested_join: ?*SupportedJoinRequest = null;
    errdefer if (nested_join) |nested| {
        nested.deinit(alloc);
        alloc.destroy(nested);
    };
    if (join.nested_join) |value| {
        nested_join = try alloc.create(SupportedJoinRequest);
        nested_join.?.* = try parseSupportedJoinClauseValue(alloc, value);
    }

    return .{
        .right_table = right_table,
        .join_type = switch (join.join_type orelse .inner) {
            .inner => .inner,
            .left => .left,
            .right => .right,
        },
        .left_field = left_field,
        .right_field = right_field,
        .right_filters = right_filters,
        .right_fields = right_fields,
        .strategy_hint = strategy_hint,
        .nested_join = nested_join,
        .operator = join.on.operator orelse .eq,
    };
}

fn supportedJoinFiltersFromOpenApi(
    alloc: std.mem.Allocator,
    filters: ?metadata_openapi.JoinFilters,
) !?SupportedJoinFilters {
    const value = filters orelse return null;
    return .{
        .filter_query = if (value.filter_query) |query| try cloneJsonValue(alloc, query) else null,
        .filter_prefix = if (value.filter_prefix) |prefix| try alloc.dupe(u8, prefix) else null,
        .limit = if (value.limit) |limit|
            if (limit < 0)
                return error.InvalidQueryRequest
            else
                std.math.cast(usize, limit) orelse return error.InvalidQueryRequest
        else
            null,
    };
}

pub fn rewriteJoinedBaseQueryBodyAlloc(
    alloc: std.mem.Allocator,
    request: metadata_openapi.QueryRequest,
    join_left_field: []const u8,
) !JoinedBaseQueryRewrite {
    var effective_fields = try maybeAppendRequestedFieldAlloc(alloc, request.fields, join_left_field);
    defer effective_fields.deinit(alloc);

    var base_request = request;
    base_request.fields = effective_fields.values;
    base_request.join = null;
    base_request.foreign_sources = null;

    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try std.json.Stringify.value(base_request, .{}, &out.writer);
    return .{
        .body = try out.toOwnedSlice(),
        .appended_left_field = effective_fields.appended,
    };
}

fn maybeAppendRequestedFieldAlloc(
    alloc: std.mem.Allocator,
    fields: ?[]const []const u8,
    field_name: []const u8,
) !OwnedRequestedFields {
    const existing_fields = fields orelse return .{ .values = null };
    if (std.mem.eql(u8, field_name, "_id")) {
        return .{ .values = existing_fields };
    }
    for (existing_fields) |field| {
        if (std.mem.eql(u8, field, field_name)) {
            return .{ .values = existing_fields };
        }
    }

    const owned = try alloc.alloc([]u8, existing_fields.len + 1);
    errdefer alloc.free(owned);
    var initialized: usize = 0;
    errdefer {
        for (owned[0..initialized]) |value| alloc.free(value);
    }
    for (existing_fields, 0..) |field, idx| {
        owned[idx] = try alloc.dupe(u8, field);
        initialized += 1;
    }
    owned[existing_fields.len] = try alloc.dupe(u8, field_name);
    return .{
        .values = owned,
        .owned = owned,
        .appended = true,
    };
}

pub fn encodeJoinPartitionRequest(
    alloc: std.mem.Allocator,
    job_id: ?u64,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    appended_left_field: bool,
    partition_index: usize,
    partition_count: usize,
    right_group_ids: []const u64,
) ![]u8 {
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer deinitJsonValue(alloc, &root);
    if (job_id) |value| try putOwnedJsonU64Field(alloc, &root.object, "job_id", value);
    try putOwnedJsonField(alloc, &root.object, "join", try encodeSupportedJoinClauseValue(alloc, join));
    try putOwnedJsonField(alloc, &root.object, "appended_left_field", .{ .bool = appended_left_field });
    try putOwnedJsonField(alloc, &root.object, "partition_index", .{ .integer = @intCast(partition_index) });
    try putOwnedJsonField(alloc, &root.object, "partition_count", .{ .integer = @intCast(partition_count) });
    var hits_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &hits_value);
    for (left_hits) |hit| {
        try hits_value.array.append(try cloneJsonValue(alloc, hit));
    }
    try putOwnedJsonField(alloc, &root.object, "left_hits", hits_value);
    var groups_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &groups_value);
    for (right_group_ids) |group_id| {
        try groups_value.array.append(try jsonU64ValueAlloc(alloc, group_id));
    }
    try putOwnedJsonField(alloc, &root.object, "right_group_ids", groups_value);
    return try stringifyJsonValueAlloc(alloc, root);
}

fn encodeJoinJobStateRequest(
    alloc: std.mem.Allocator,
    job_id: u64,
) ![]u8 {
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer deinitJsonValue(alloc, &root);
    try putOwnedJsonU64Field(alloc, &root.object, "job_id", job_id);
    return try stringifyJsonValueAlloc(alloc, root);
}

fn parseJoinPartitionRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !JoinPartitionRequest {
    var parsed = try std.json.parseFromSlice(EncodedJoinPartitionRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    return .{
        .job_id = parsed.value.job_id,
        .join = try supportedJoinRequestFromOpenApi(alloc, parsed.value.join),
        .left_hits = parsed.value.left_hits,
        .appended_left_field = parsed.value.appended_left_field orelse false,
        .partition_index = if (parsed.value.partition_index) |value|
            std.math.cast(usize, value) orelse return error.InvalidQueryRequest
        else
            0,
        .partition_count = if (parsed.value.partition_count) |value|
            if (value == 0)
                return error.InvalidQueryRequest
            else
                std.math.cast(usize, value) orelse return error.InvalidQueryRequest
        else
            1,
        .right_group_ids = parsed.value.right_group_ids orelse &.{},
        .parsed = parsed,
    };
}

fn encodeJoinRowsRequest(
    alloc: std.mem.Allocator,
    job_id: ?u64,
    join: SupportedJoinRequest,
    partition_index: usize,
    partition_count: usize,
) ![]u8 {
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer deinitJsonValue(alloc, &root);
    if (job_id) |value| try putOwnedJsonU64Field(alloc, &root.object, "job_id", value);
    try putOwnedJsonField(alloc, &root.object, "join", try encodeSupportedJoinClauseValue(alloc, join));
    try putOwnedJsonField(alloc, &root.object, "partition_index", .{ .integer = @intCast(partition_index) });
    try putOwnedJsonField(alloc, &root.object, "partition_count", .{ .integer = @intCast(partition_count) });
    return try stringifyJsonValueAlloc(alloc, root);
}

fn encodeJoinUnmatchedRequest(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
    left_hit_count: usize,
    left_fields: []const []const u8,
    appended_left_field: bool,
    matched_right_ids: []const []const u8,
) ![]u8 {
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer deinitJsonValue(alloc, &root);
    try putOwnedJsonField(alloc, &root.object, "join", try encodeSupportedJoinClauseValue(alloc, join));
    try putOwnedJsonField(alloc, &root.object, "appended_left_field", .{ .bool = appended_left_field });
    try putOwnedJsonField(alloc, &root.object, "left_hit_count", .{ .integer = @intCast(left_hit_count) });

    var left_fields_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &left_fields_value);
    for (left_fields) |field| try left_fields_value.array.append(.{ .string = try alloc.dupe(u8, field) });
    try putOwnedJsonField(alloc, &root.object, "left_fields", left_fields_value);

    var matched_ids_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &matched_ids_value);
    for (matched_right_ids) |matched_id| try matched_ids_value.array.append(.{ .string = try alloc.dupe(u8, matched_id) });
    try putOwnedJsonField(alloc, &root.object, "matched_right_ids", matched_ids_value);

    return try stringifyJsonValueAlloc(alloc, root);
}

pub fn encodeJoinFinalizeRequest(
    alloc: std.mem.Allocator,
    job_id: u64,
    handoff_owner_group_id: ?u64,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
    left_fields: []const std.json.Value,
    appended_left_field: bool,
    shuffle_partitions: usize,
) ![]u8 {
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer deinitJsonValue(alloc, &root);
    try putOwnedJsonU64Field(alloc, &root.object, "job_id", job_id);
    if (handoff_owner_group_id) |value| try putOwnedJsonField(alloc, &root.object, "handoff_owner_group_id", .{ .integer = @intCast(value) });
    try putOwnedJsonField(alloc, &root.object, "join", try encodeSupportedJoinClauseValue(alloc, join));
    try putOwnedJsonField(alloc, &root.object, "appended_left_field", .{ .bool = appended_left_field });
    try putOwnedJsonField(alloc, &root.object, "shuffle_partitions", .{ .integer = @intCast(shuffle_partitions) });
    var hits_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &hits_value);
    for (left_hits) |hit| {
        try hits_value.array.append(try cloneJsonValue(alloc, hit));
    }
    try putOwnedJsonField(alloc, &root.object, "left_hits", hits_value);
    var left_fields_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &left_fields_value);
    for (left_fields) |field| {
        try left_fields_value.array.append(try cloneJsonValue(alloc, field));
    }
    try putOwnedJsonField(alloc, &root.object, "left_fields", left_fields_value);
    return try stringifyJsonValueAlloc(alloc, root);
}

fn parseJoinRowsRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !JoinRowsRequest {
    var parsed = try std.json.parseFromSlice(EncodedJoinRowsRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    return .{
        .job_id = parsed.value.job_id,
        .join = try supportedJoinRequestFromOpenApi(alloc, parsed.value.join),
        .partition_index = if (parsed.value.partition_index) |value|
            std.math.cast(usize, value) orelse return error.InvalidQueryRequest
        else
            0,
        .partition_count = if (parsed.value.partition_count) |value|
            if (value == 0)
                return error.InvalidQueryRequest
            else
                std.math.cast(usize, value) orelse return error.InvalidQueryRequest
        else
            1,
        .parsed = parsed,
    };
}

fn parseJoinUnmatchedRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !JoinUnmatchedRequest {
    var parsed = try std.json.parseFromSlice(EncodedJoinUnmatchedRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    return .{
        .join = try supportedJoinRequestFromOpenApi(alloc, parsed.value.join),
        .left_hit_count = if (parsed.value.left_hit_count) |value|
            std.math.cast(usize, value) orelse return error.InvalidQueryRequest
        else
            0,
        .left_fields = parsed.value.left_fields orelse &.{},
        .appended_left_field = parsed.value.appended_left_field orelse false,
        .matched_right_ids = parsed.value.matched_right_ids orelse &.{},
        .parsed = parsed,
    };
}

fn parseJoinFinalizeRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !JoinFinalizeRequest {
    var parsed = try std.json.parseFromSlice(EncodedJoinFinalizeRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    errdefer parsed.deinit();
    return .{
        .job_id = parsed.value.job_id,
        .handoff_owner_group_id = parsed.value.handoff_owner_group_id,
        .join = try supportedJoinRequestFromOpenApi(alloc, parsed.value.join),
        .left_hits = parsed.value.left_hits,
        .left_fields = parsed.value.left_fields orelse &.{},
        .appended_left_field = parsed.value.appended_left_field orelse false,
        .shuffle_partitions = if (parsed.value.shuffle_partitions) |value|
            if (value == 0)
                return error.InvalidQueryRequest
            else
                std.math.cast(usize, value) orelse return error.InvalidQueryRequest
        else
            1,
        .parsed = parsed,
    };
}

fn parseJoinJobStateRequest(
    alloc: std.mem.Allocator,
    body: []const u8,
) !u64 {
    var parsed = try std.json.parseFromSlice(EncodedJoinJobStateRequest, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    return parsed.value.job_id;
}

fn encodeJoinRowsResponse(
    alloc: std.mem.Allocator,
    hits: []const std.json.Value,
) ![]u8 {
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer deinitJsonValue(alloc, &root);
    var hits_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &hits_value);
    for (hits) |hit| {
        try hits_value.array.append(try cloneJsonValue(alloc, hit));
    }
    try putOwnedJsonField(alloc, &root.object, "hits", hits_value);
    return try stringifyJsonValueAlloc(alloc, root);
}

fn encodeJoinUnmatchedResponse(
    alloc: std.mem.Allocator,
    hits: []const std.json.Value,
    right_rows_scanned: u64,
) ![]u8 {
    var root = std.json.Value{ .object = std.json.ObjectMap.empty };
    defer deinitJsonValue(alloc, &root);
    var hits_value = std.json.Value{ .array = std.json.Array.init(alloc) };
    errdefer deinitJsonValue(alloc, &hits_value);
    for (hits) |hit| try hits_value.array.append(try cloneJsonValue(alloc, hit));
    try putOwnedJsonField(alloc, &root.object, "hits", hits_value);
    try putOwnedJsonField(alloc, &root.object, "right_rows_scanned", .{ .integer = @intCast(right_rows_scanned) });
    return try stringifyJsonValueAlloc(alloc, root);
}

fn parseJoinUnmatchedResponse(
    alloc: std.mem.Allocator,
    body: []const u8,
) !EncodedJoinUnmatchedResponse {
    var parsed = try std.json.parseFromSlice(EncodedJoinUnmatchedResponse, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    const owned_hits = try alloc.alloc(std.json.Value, parsed.value.hits.len);
    errdefer alloc.free(owned_hits);
    for (parsed.value.hits, 0..) |item, i| {
        owned_hits[i] = try cloneJsonValue(alloc, item);
    }
    return .{
        .hits = owned_hits,
        .right_rows_scanned = parsed.value.right_rows_scanned,
    };
}

fn parseJoinRowsResponse(
    alloc: std.mem.Allocator,
    body: []const u8,
) ![]std.json.Value {
    var parsed = try std.json.parseFromSlice(EncodedJoinRowsResponse, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    const owned_hits = try alloc.alloc(std.json.Value, parsed.value.hits.len);
    errdefer alloc.free(owned_hits);
    for (parsed.value.hits, 0..) |item, i| {
        owned_hits[i] = try cloneJsonValue(alloc, item);
    }
    return owned_hits;
}

pub fn encodeJoinJobState(alloc: std.mem.Allocator, job_id: u64, state: JoinShuffleJobState) ![]u8 {
    var out: std.Io.Writer.Allocating = .init(alloc);
    defer out.deinit();
    try std.json.Stringify.value(EncodedJoinJobState{
        .job_id = job_id,
        .owner_group_id = state.owner_group_id,
        .phase = phaseString(state.phase),
        .total_partitions = state.total_partitions,
        .completed_partitions = state.completed_partitions,
        .next_partition_index = state.next_partition_index,
        .worker_retries = state.worker_retries,
        .finalizer_retries = state.finalizer_retries,
        .coordinator_finalized = state.coordinator_finalized,
        .last_updated_at_millis = state.last_updated_at_millis,
        .expires_at_millis = state.expires_at_millis,
        .last_error = state.last_error,
        .partial_response = state.partial_response,
        .cached_response = state.cached_response,
    }, .{}, &out.writer);
    return try out.toOwnedSlice();
}

fn parseJoinJobState(alloc: std.mem.Allocator, body: []const u8) !JoinShuffleJobState {
    var parsed = try std.json.parseFromSlice(EncodedJoinJobState, alloc, body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    const last_error = if (parsed.value.last_error) |value| try alloc.dupe(u8, value) else null;
    errdefer if (last_error) |value| alloc.free(value);
    const partial_response = if (parsed.value.partial_response) |value| try alloc.dupe(u8, value) else null;
    errdefer if (partial_response) |value| alloc.free(value);
    const cached_response = if (parsed.value.cached_response) |value| try alloc.dupe(u8, value) else null;
    errdefer if (cached_response) |value| alloc.free(value);
    return .{
        .owner_group_id = parsed.value.owner_group_id,
        .phase = try phaseFromString(parsed.value.phase),
        .total_partitions = std.math.cast(usize, parsed.value.total_partitions) orelse return error.InvalidQueryRequest,
        .completed_partitions = std.math.cast(usize, parsed.value.completed_partitions) orelse return error.InvalidQueryRequest,
        .next_partition_index = if (parsed.value.next_partition_index) |value|
            std.math.cast(usize, value) orelse return error.InvalidQueryRequest
        else
            0,
        .worker_retries = std.math.cast(usize, parsed.value.worker_retries) orelse return error.InvalidQueryRequest,
        .finalizer_retries = std.math.cast(usize, parsed.value.finalizer_retries) orelse return error.InvalidQueryRequest,
        .coordinator_finalized = parsed.value.coordinator_finalized,
        .last_updated_at_millis = parsed.value.last_updated_at_millis,
        .expires_at_millis = parsed.value.expires_at_millis,
        .last_error = last_error,
        .partial_response = partial_response,
        .cached_response = cached_response,
    };
}

// -- query helpers --

pub fn queryHitsArrayPtr(root: *std.json.Value) !*std.json.Array {
    return try join_model.queryHitsArrayPtr(root);
}

fn queryTotalHits(root: std.json.Value) !usize {
    if (root != .object) return error.InvalidQueryRequest;
    const responses = root.object.get("responses") orelse return error.InvalidQueryRequest;
    if (responses != .array or responses.array.items.len == 0) return error.InvalidQueryRequest;
    const response = responses.array.items[0];
    if (response != .object) return error.InvalidQueryRequest;
    const hits = response.object.get("hits") orelse return error.InvalidQueryRequest;
    if (hits != .object) return error.InvalidQueryRequest;
    const total = hits.object.get("total") orelse return error.InvalidQueryRequest;
    return switch (total) {
        .integer => |value| @intCast(value),
        else => error.InvalidQueryRequest,
    };
}

pub fn extractJoinValueFromHit(hit: std.json.Value, field_name: []const u8) ?std.json.Value {
    if (hit != .object) return null;
    if (std.mem.eql(u8, field_name, "_id")) return hit.object.get("_id");
    const source = hit.object.get("_source") orelse return null;
    return extractJsonPathValue(source, field_name);
}

pub fn extractJsonPathValue(value: std.json.Value, path: []const u8) ?std.json.Value {
    return json_helpers.extractJsonPathValue(value, path);
}

// -- join merge/apply helpers --

pub fn applyJoinedRightHitsToResponse(
    alloc: std.mem.Allocator,
    root: *std.json.Value,
    left_hits: []const std.json.Value,
    join: SupportedJoinRequest,
    right_hits: []const std.json.Value,
    requested_left_fields: anytype,
    appended_left_field: bool,
) !JoinedQueryStats {
    var merged = try mergeJoinedRightHitsAlloc(
        alloc,
        left_hits,
        join,
        right_hits,
        requested_left_fields,
        appended_left_field,
    );
    defer merged.deinit(alloc);
    try join_model.applyJoinShellToResponse(alloc, root, merged);
    return merged.stats;
}

pub fn mergeJoinedRightHitsAlloc(
    alloc: std.mem.Allocator,
    left_hits: []const std.json.Value,
    join: SupportedJoinRequest,
    right_hits: []const std.json.Value,
    requested_left_fields: anytype,
    appended_left_field: bool,
) !JoinedRightMergeResult {
    var stats: JoinedQueryStats = .{
        .left_rows_scanned = @intCast(left_hits.len),
        .right_rows_scanned = @intCast(right_hits.len),
    };
    var matched_right_ids = std.StringHashMapUnmanaged(void){};
    defer matched_right_ids.deinit(alloc);

    var joined_hits = std.json.Array.init(alloc);
    errdefer {
        for (joined_hits.items) |*item| deinitJsonValue(alloc, item);
        joined_hits.deinit();
    }

    for (left_hits) |hit_value| {
        var joined_hit = try cloneJsonValue(alloc, hit_value);
        errdefer deinitJsonValue(alloc, &joined_hit);
        const source_value = joined_hit.object.getPtr("_source") orelse return error.InvalidQueryRequest;
        if (source_value.* != .object) return error.InvalidQueryRequest;
        if (appended_left_field) removeFieldFromSourceObject(alloc, source_value, join.left_field);

        const left_value = extractJoinValueFromHit(hit_value, join.left_field) orelse {
            stats.rows_unmatched_left += 1;
            if (join.join_type == .left) try joined_hits.append(joined_hit);
            continue;
        };
        const matched_right = findFirstMatchingRightHit(join, left_value, right_hits) orelse {
            stats.rows_unmatched_left += 1;
            if (join.join_type == .left) try joined_hits.append(joined_hit);
            continue;
        };

        try mergeRightHitIntoSource(alloc, source_value, join, matched_right);
        if (matched_right.object.get("_id")) |matched_id| {
            if (matched_id == .string) try matched_right_ids.put(alloc, matched_id.string, {});
        }
        try joined_hits.append(joined_hit);
        stats.rows_matched += 1;
    }

    if (join.join_type == .right) {
        stats.rows_unmatched_right += @intCast(try join_model.appendUnmatchedRightJoinHitsAlloc(
            alloc,
            &joined_hits,
            right_hits,
            join,
            requested_left_fields,
            appended_left_field,
            &matched_right_ids,
        ));
    }

    return try join_model.adoptOwnedJoinShellAlloc(
        JoinedQueryStats,
        alloc,
        try joined_hits.toOwnedSlice(),
        stats,
        &matched_right_ids,
    );
}

// -- profiling --

pub fn maybeAttachJoinProfile(
    alloc: std.mem.Allocator,
    root: *std.json.Value,
    stats: JoinedQueryStats,
    plan: PlannedJoinExecution,
    strategy_used: RightJoinQueryResult.StrategyUsed,
    distributed_execution: bool,
    groups_queried: usize,
) !void {
    const payload: join_model.JoinProfilePayload = .{
        .strategy_used = joinStrategyUsedString(strategy_used),
        .left_rows_scanned = stats.left_rows_scanned,
        .right_rows_scanned = stats.right_rows_scanned,
        .rows_matched = stats.rows_matched,
        .rows_unmatched_left = stats.rows_unmatched_left,
        .rows_unmatched_right = stats.rows_unmatched_right,
        .estimated_cost = plan.estimated_cost,
        .estimated_rows = plan.estimated_rows,
        .estimated_memory_bytes = plan.estimated_memory_bytes,
        .planner_used_stats = plan.used_stats,
        .distributed_execution = distributed_execution,
        .groups_queried = groups_queried,
        .shuffle_partitions = plan.shuffle_partitions,
        .shuffle_candidate = plan.shuffle_candidate,
        .forced_broadcast_fallback = plan.forced_broadcast_fallback,
    };
    try join_model.applyJoinProfileToResponse(alloc, root, payload);
}

pub fn maybeAttachJoinWorkerExecution(
    alloc: std.mem.Allocator,
    root: *std.json.Value,
    result: JoinPartitionExecutionResult,
) !void {
    const join_ptr = responseJoinProfileObjectPtr(root) orelse return;
    const worker_attempts = try alloc.alloc(join_model.JoinWorkerAttemptPayload, result.worker_attempts.len);
    defer if (worker_attempts.len > 0) alloc.free(worker_attempts);
    for (result.worker_attempts, 0..) |attempt, i| {
        worker_attempts[i] = .{
            .partition_index = attempt.partition_index,
            .worker_group_id = attempt.worker_group_id,
            .succeeded = attempt.succeeded,
        };
    }

    const finalizer_attempts = try alloc.alloc(join_model.JoinFinalizerAttemptPayload, result.finalizer_attempts.len);
    defer if (finalizer_attempts.len > 0) alloc.free(finalizer_attempts);
    for (result.finalizer_attempts, 0..) |attempt, i| {
        finalizer_attempts[i] = .{
            .worker_group_id = attempt.worker_group_id,
            .succeeded = attempt.succeeded,
        };
    }

    const payload: join_model.JoinWorkerExecutionPayload = .{
        .execution_mode = joinExecutionModeString(result.execution_mode),
        .job_id = result.job_id,
        .job_phase = if (result.job_phase) |phase| phaseString(phase) else null,
        .total_partitions = result.total_partitions,
        .completed_partitions = result.completed_partitions,
        .expires_at_millis = result.expires_at_millis,
        .worker_retries = result.worker_retries,
        .finalizer_retries = result.finalizer_retries,
        .finalizer_group_id = result.finalizer_group_id,
        .coordinator_finalized = result.coordinator_finalized,
        .imported_owner_group_id = result.imported_owner_group_id,
        .imported_partial_state = result.imported_partial_state,
        .imported_cached_result = result.imported_cached_result,
        .worker_attempts = worker_attempts,
        .finalizer_attempts = finalizer_attempts,
    };
    try join_model.applyJoinWorkerExecutionPayload(alloc, &join_ptr.object, payload);
}

// -- strategy/planning helpers --

fn chooseJoinExecutionStrategyWithStats(
    plan: *PlannedJoinExecution,
    join: SupportedJoinRequest,
    supported_index_lookup: bool,
    left_rows: u64,
    distinct_left_keys: u64,
    left_stats: JoinTableStats,
    right_stats: JoinTableStats,
) void {
    chooseStatefulJoinStrategyWithStats(
        join,
        supported_index_lookup,
        left_rows,
        distinct_left_keys,
        left_stats,
        right_stats,
    ).apply(plan);
}

fn chooseStatefulJoinStrategyWithStats(
    join: SupportedJoinRequest,
    supported_index_lookup: bool,
    left_rows: u64,
    distinct_left_keys: u64,
    left_stats: JoinTableStats,
    right_stats: JoinTableStats,
) StatefulJoinStrategyDecision {
    if (join_model.joinSideBelowBroadcastThreshold(right_stats.size_bytes)) {
        return .{ .strategy = .broadcast };
    } else if (join_model.joinSideBelowBroadcastThreshold(left_stats.size_bytes)) {
        return .{ .strategy = .broadcast };
    } else if (supported_index_lookup and right_stats.row_count > 0) {
        const simple_strategy = join_model.chooseBroadcastOrIndexLookupWithStats(
            RightJoinQueryResult.StrategyUsed,
            supported_index_lookup,
            distinct_left_keys,
            right_stats.row_count,
            right_stats.size_bytes,
        );
        if (simple_strategy == .index_lookup) {
            return .{ .strategy = .index_lookup };
        } else {
            return chooseStatefulShuffleCandidateOrBroadcast(join, left_rows, left_stats.size_bytes, right_stats.size_bytes);
        }
    } else {
        return chooseStatefulShuffleCandidateOrBroadcast(join, left_rows, left_stats.size_bytes, right_stats.size_bytes);
    }
}

fn chooseStatefulShuffleCandidateOrBroadcast(
    join: SupportedJoinRequest,
    left_rows: u64,
    left_size_bytes: u64,
    right_size_bytes: u64,
) StatefulJoinStrategyDecision {
    if (join_model.joinSidesNeedShuffleCandidate(left_size_bytes, right_size_bytes)) {
        return chooseStatefulShuffleStrategyOrForcedBroadcast(join, left_rows, right_size_bytes);
    } else {
        return .{ .strategy = .broadcast };
    }
}

fn chooseStatefulShuffleStrategyOrForcedBroadcast(
    join: SupportedJoinRequest,
    left_rows: u64,
    right_size_bytes: u64,
) StatefulJoinStrategyDecision {
    if (join.join_type == .right) {
        return .{
            .strategy = .broadcast,
            .shuffle_candidate = true,
            .forced_broadcast_fallback = true,
        };
    }
    return .{
        .strategy = .shuffle,
        .shuffle_partitions = calculateShufflePartitions(left_rows, right_size_bytes),
        .shuffle_candidate = true,
    };
}

fn estimateJoinTableStatsFromSnapshot(
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
) JoinTableStats {
    const table = tables_api.findTableByName(snapshot, table_name) orelse return .{};
    var stats: JoinTableStats = .{};
    for (snapshot.ranges) |range| {
        if (range.table_id != table.table_id) continue;
        stats.shard_count += 1;
        for (snapshot.merged_group_statuses) |status| {
            if (status.group_id != range.group_id) continue;
            stats.row_count += status.doc_count;
            stats.size_bytes += status.disk_bytes;
            stats.has_stats = true;
            break;
        }
    }
    return stats;
}

fn estimateJoinPlanCosts(
    plan: *PlannedJoinExecution,
    strategy: RightJoinQueryResult.StrategyUsed,
    left_rows: u64,
    right_rows: u64,
    right_size_bytes: u64,
) void {
    plan.estimated_rows = left_rows;
    switch (strategy) {
        .broadcast => {
            join_model.applyBroadcastJoinPlanCost(
                &plan.estimated_cost,
                &plan.estimated_memory_bytes,
                left_rows,
                right_rows,
                right_size_bytes,
            );
        },
        .index_lookup => {
            join_model.applyIndexLookupJoinPlanCostBatched(
                &plan.estimated_cost,
                &plan.estimated_memory_bytes,
                left_rows,
            );
        },
        .shuffle => {
            plan.estimated_cost = @as(f64, @floatFromInt(left_rows + right_rows)) * 2;
            plan.estimated_memory_bytes = if (plan.shuffle_partitions > 0)
                ((left_rows + right_rows) * join_model.join_estimated_row_bytes) / @as(u64, @intCast(plan.shuffle_partitions))
            else
                (left_rows + right_rows) * join_model.join_estimated_row_bytes;
        },
    }
}

fn calculateShufflePartitions(left_rows: u64, right_size_bytes: u64) usize {
    const target_partition_bytes: u64 = 10 * 1024 * 1024;
    const estimated_left_bytes = left_rows * 200;
    const total_bytes = estimated_left_bytes + right_size_bytes;
    const raw = if (total_bytes == 0) 1 else (total_bytes + target_partition_bytes - 1) / target_partition_bytes;
    const bounded = @max(@as(u64, 1), @min(@as(u64, 128), raw));
    return @intCast(bounded);
}

pub fn partitionForJoinValue(value: std.json.Value, partition_count: usize) usize {
    if (partition_count <= 1) return 0;
    var hasher = std.hash.Wyhash.init(0);
    switch (value) {
        .null => hasher.update("null"),
        .bool => |flag| hasher.update(if (flag) "true" else "false"),
        .integer => |number| {
            var buf: [32]u8 = undefined;
            const text = std.fmt.bufPrint(&buf, "{d}", .{number}) catch "0";
            hasher.update(text);
        },
        .float => |number| {
            var buf: [64]u8 = undefined;
            const text = std.fmt.bufPrint(&buf, "{d}", .{number}) catch "0";
            hasher.update(text);
        },
        .number_string => |text| hasher.update(text),
        .string => |text| hasher.update(text),
        else => hasher.update("unsupported"),
    }
    return @intCast(hasher.final() % partition_count);
}

fn collectLeftHitsForJoinPartitionAlloc(
    alloc: std.mem.Allocator,
    left_hits: []const std.json.Value,
    left_field: []const u8,
    partition_index: usize,
    partition_count: usize,
) ![]std.json.Value {
    var out = std.json.Array.init(alloc);
    errdefer out.deinit();
    for (left_hits) |hit| {
        const left_value = extractJoinValueFromHit(hit, left_field) orelse continue;
        if (partitionForJoinValue(left_value, partition_count) != partition_index) continue;
        try out.append(hit);
    }
    return try out.toOwnedSlice();
}

fn appendClonedJsonHitsToArray(
    alloc: std.mem.Allocator,
    out: *std.json.Array,
    hits: []const std.json.Value,
) !void {
    for (hits) |hit| {
        try out.append(try cloneJsonValue(alloc, hit));
    }
}

// -- join query building --

fn buildRightJoinQueryValue(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
    left_hits: []const std.json.Value,
) !std.json.Value {
    const filter_query_value = blk: {
        if (join.join_type == .right) {
            if (join.right_filters) |filters| {
                if (filters.filter_query) |filter_query| break :blk try cloneJsonValue(alloc, filter_query);
            }
            break :blk null;
        }

        var disjuncts = std.json.Array.init(alloc);
        errdefer {
            for (disjuncts.items) |*item| deinitJsonValue(alloc, item);
            disjuncts.deinit();
        }

        for (left_hits) |hit_value| {
            const left_value = extractJoinValueFromHit(hit_value, join.left_field) orelse continue;
            try disjuncts.append(try buildJoinEqualityQuery(alloc, join.right_field, left_value));
        }

        var join_filter_obj = std.json.ObjectMap.empty;
        try join_filter_obj.put(alloc, try alloc.dupe(u8, "disjuncts"), .{ .array = disjuncts });
        if (join.right_filters) |filters| {
            break :blk try buildCombinedRightFilterQueryValue(alloc, filters, .{ .object = join_filter_obj });
        }
        break :blk std.json.Value{ .object = join_filter_obj };
    };

    var root = std.json.ObjectMap.empty;
    if (filter_query_value) |filter_query| {
        try root.put(alloc, try alloc.dupe(u8, "filter_query"), filter_query);
    }
    if (filter_query_value != null or join.join_type == .right) {
        try root.put(alloc, try alloc.dupe(u8, "full_text_search"), try buildMatchAllQueryValue(alloc));
    }
    const requested_limit = if (join.right_filters) |filters| filters.limit else null;
    try root.put(alloc, try alloc.dupe(u8, "limit"), .{ .integer = @intCast(requested_limit orelse @max(@as(usize, 10), left_hits.len)) });
    if (join.right_filters) |filters| {
        if (filters.filter_prefix) |prefix| {
            try root.put(alloc, try alloc.dupe(u8, "filter_prefix"), .{ .string = try alloc.dupe(u8, prefix) });
        }
    }
    if (join.right_fields.len > 0 or !std.mem.eql(u8, join.right_field, "_id")) {
        var fields = std.json.Array.init(alloc);
        var saw_join_field = false;
        var saw_nested_left_field = false;
        for (join.right_fields) |field| {
            if (join.nested_join != null and std.mem.indexOfScalar(u8, field, '.') != null) continue;
            try fields.append(.{ .string = try alloc.dupe(u8, field) });
            if (std.mem.eql(u8, field, join.right_field)) saw_join_field = true;
            if (join.nested_join) |nested| {
                if (std.mem.eql(u8, field, nested.left_field)) saw_nested_left_field = true;
            }
        }
        if (!std.mem.eql(u8, join.right_field, "_id") and !saw_join_field) {
            try fields.append(.{ .string = try alloc.dupe(u8, join.right_field) });
        }
        if (join.nested_join) |nested| {
            if (!std.mem.eql(u8, nested.left_field, "_id") and !saw_nested_left_field) {
                try fields.append(.{ .string = try alloc.dupe(u8, nested.left_field) });
            }
        }
        try root.put(alloc, try alloc.dupe(u8, "fields"), .{ .array = fields });
    }
    if (join.nested_join) |nested| {
        try root.put(alloc, try alloc.dupe(u8, "join"), try buildSupportedJoinClauseValue(alloc, nested.*));
    }
    return .{ .object = root };
}

fn buildRightJoinPartitionRowsQueryValue(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
) !std.json.Value {
    var root = std.json.ObjectMap.empty;
    var has_filter_query = false;
    if (join.right_filters) |filters| {
        if (filters.filter_query) |filter_query| {
            try root.put(alloc, try alloc.dupe(u8, "filter_query"), try cloneJsonValue(alloc, filter_query));
            has_filter_query = true;
        }
        if (filters.filter_prefix) |prefix| {
            try root.put(alloc, try alloc.dupe(u8, "filter_prefix"), .{ .string = try alloc.dupe(u8, prefix) });
        }
        if (filters.limit) |limit| {
            try root.put(alloc, try alloc.dupe(u8, "limit"), .{ .integer = @intCast(limit) });
        }
    }
    if (!has_filter_query) {
        try root.put(alloc, try alloc.dupe(u8, "full_text_search"), try buildMatchAllQueryValue(alloc));
    }
    if (join.right_fields.len > 0 or !std.mem.eql(u8, join.right_field, "_id")) {
        var fields = std.json.Array.init(alloc);
        var saw_join_field = false;
        for (join.right_fields) |field| {
            try fields.append(.{ .string = try alloc.dupe(u8, field) });
            if (std.mem.eql(u8, field, join.right_field)) saw_join_field = true;
        }
        if (!std.mem.eql(u8, join.right_field, "_id") and !saw_join_field) {
            try fields.append(.{ .string = try alloc.dupe(u8, join.right_field) });
        }
        try root.put(alloc, try alloc.dupe(u8, "fields"), .{ .array = fields });
    }
    return .{ .object = root };
}

fn buildCombinedRightFilterQueryValue(
    alloc: std.mem.Allocator,
    filters: SupportedJoinFilters,
    join_filter: std.json.Value,
) !std.json.Value {
    if (filters.filter_query) |filter_query| {
        var conjuncts = std.json.Array.init(alloc);
        errdefer {
            for (conjuncts.items) |*item| deinitJsonValue(alloc, item);
            conjuncts.deinit();
        }
        try conjuncts.append(try cloneJsonValue(alloc, filter_query));
        try conjuncts.append(join_filter);
        var obj = std.json.ObjectMap.empty;
        try obj.put(alloc, try alloc.dupe(u8, "conjuncts"), .{ .array = conjuncts });
        return .{ .object = obj };
    }
    return join_filter;
}

fn buildMatchAllQueryValue(alloc: std.mem.Allocator) !std.json.Value {
    var root = std.json.ObjectMap.empty;
    try root.put(alloc, try alloc.dupe(u8, "match_all"), .{ .object = std.json.ObjectMap.empty });
    return .{ .object = root };
}

fn buildSupportedJoinClauseValue(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
) !std.json.Value {
    var join_obj = std.json.ObjectMap.empty;
    try join_obj.put(alloc, try alloc.dupe(u8, "right_table"), .{ .string = try alloc.dupe(u8, join.right_table) });
    try join_obj.put(alloc, try alloc.dupe(u8, "join_type"), .{ .string = try alloc.dupe(u8, switch (join.join_type) {
        .inner => "inner",
        .left => "left",
        .right => "right",
    }) });

    var on_obj = std.json.ObjectMap.empty;
    try on_obj.put(alloc, try alloc.dupe(u8, "left_field"), .{ .string = try alloc.dupe(u8, join.left_field) });
    try on_obj.put(alloc, try alloc.dupe(u8, "right_field"), .{ .string = try alloc.dupe(u8, join.right_field) });
    try on_obj.put(alloc, try alloc.dupe(u8, "operator"), .{ .string = try alloc.dupe(u8, "eq") });
    try join_obj.put(alloc, try alloc.dupe(u8, "on"), .{ .object = on_obj });

    if (join.right_filters) |filters| {
        var filters_obj = std.json.ObjectMap.empty;
        if (filters.filter_query) |filter_query| {
            try filters_obj.put(alloc, try alloc.dupe(u8, "filter_query"), try cloneJsonValue(alloc, filter_query));
        }
        if (filters.filter_prefix) |prefix| {
            try filters_obj.put(alloc, try alloc.dupe(u8, "filter_prefix"), .{ .string = try alloc.dupe(u8, prefix) });
        }
        if (filters.limit) |limit| {
            try filters_obj.put(alloc, try alloc.dupe(u8, "limit"), .{ .integer = @intCast(limit) });
        }
        try join_obj.put(alloc, try alloc.dupe(u8, "right_filters"), .{ .object = filters_obj });
    }

    if (join.right_fields.len > 0) {
        var fields = std.json.Array.init(alloc);
        for (join.right_fields) |field| {
            try fields.append(.{ .string = try alloc.dupe(u8, field) });
        }
        try join_obj.put(alloc, try alloc.dupe(u8, "right_fields"), .{ .array = fields });
    }

    if (join.strategy_hint) |hint| {
        try join_obj.put(alloc, try alloc.dupe(u8, "strategy_hint"), .{ .string = try alloc.dupe(u8, hint) });
    }

    if (join.nested_join) |nested| {
        try join_obj.put(alloc, try alloc.dupe(u8, "nested_join"), try buildSupportedJoinClauseValue(alloc, nested.*));
    }

    return .{ .object = join_obj };
}

fn buildJoinEqualityQuery(
    alloc: std.mem.Allocator,
    field_name: []const u8,
    value: std.json.Value,
) !std.json.Value {
    var query_obj = std.json.ObjectMap.empty;
    switch (value) {
        .string => |text| {
            var term_obj = std.json.ObjectMap.empty;
            try term_obj.put(alloc, try alloc.dupe(u8, field_name), .{ .string = try alloc.dupe(u8, text) });
            try query_obj.put(alloc, try alloc.dupe(u8, "term"), .{ .object = term_obj });
        },
        .integer => |number| {
            var range_obj = std.json.ObjectMap.empty;
            try range_obj.put(alloc, try alloc.dupe(u8, "field"), .{ .string = try alloc.dupe(u8, field_name) });
            try range_obj.put(alloc, try alloc.dupe(u8, "min"), .{ .integer = number });
            try range_obj.put(alloc, try alloc.dupe(u8, "max"), .{ .integer = number });
            try range_obj.put(alloc, try alloc.dupe(u8, "inclusive_min"), .{ .bool = true });
            try range_obj.put(alloc, try alloc.dupe(u8, "inclusive_max"), .{ .bool = true });
            try query_obj.put(alloc, try alloc.dupe(u8, "numeric_range"), .{ .object = range_obj });
        },
        .float => |number| {
            var range_obj = std.json.ObjectMap.empty;
            try range_obj.put(alloc, try alloc.dupe(u8, "field"), .{ .string = try alloc.dupe(u8, field_name) });
            try range_obj.put(alloc, try alloc.dupe(u8, "min"), .{ .float = number });
            try range_obj.put(alloc, try alloc.dupe(u8, "max"), .{ .float = number });
            try range_obj.put(alloc, try alloc.dupe(u8, "inclusive_min"), .{ .bool = true });
            try range_obj.put(alloc, try alloc.dupe(u8, "inclusive_max"), .{ .bool = true });
            try query_obj.put(alloc, try alloc.dupe(u8, "numeric_range"), .{ .object = range_obj });
        },
        .bool => |flag| {
            try query_obj.put(alloc, try alloc.dupe(u8, "field"), .{ .string = try alloc.dupe(u8, field_name) });
            try query_obj.put(alloc, try alloc.dupe(u8, "bool"), .{ .bool = flag });
        },
        else => return error.UnsupportedQueryRequest,
    }
    return .{ .object = query_obj };
}

fn attachJoinForeignSourcesToQueryValue(
    alloc: std.mem.Allocator,
    query_value: *std.json.Value,
    join: SupportedJoinRequest,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !void {
    if (join.nested_join == null or foreign_sources.isEmpty()) return;
    if (query_value.* != .object) return error.InvalidQueryRequest;
    try query_value.object.put(alloc, try alloc.dupe(u8, "foreign_sources"), try encodeForeignSourcesValueAlloc(alloc, foreign_sources));
}

fn joinUsesNestedForeignLeafLocalFallback(
    join: SupportedJoinRequest,
    foreign_sources: foreign_mod.PostgresSourceMap,
) bool {
    const nested = join.nested_join orelse return false;
    return nested.join_type == .left and nested.nested_join == null and foreign_sources.contains(nested.right_table);
}

fn encodeForeignSourcesValueAlloc(
    alloc: std.mem.Allocator,
    foreign_sources: foreign_mod.PostgresSourceMap,
) !std.json.Value {
    var root = std.json.ObjectMap.empty;
    for (foreign_sources.entries) |entry| {
        var source_obj = std.json.ObjectMap.empty;
        try source_obj.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, "postgres") });
        try source_obj.put(alloc, try alloc.dupe(u8, "dsn"), .{ .string = try alloc.dupe(u8, entry.config.dsn) });
        try source_obj.put(alloc, try alloc.dupe(u8, "postgres_table"), .{ .string = try alloc.dupe(u8, entry.config.postgres_table) });
        if (entry.config.columns.len > 0) {
            var columns = std.json.Array.init(alloc);
            for (entry.config.columns) |column| {
                var column_obj = std.json.ObjectMap.empty;
                try column_obj.put(alloc, try alloc.dupe(u8, "name"), .{ .string = try alloc.dupe(u8, column.name) });
                try column_obj.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, column.data_type) });
                try column_obj.put(alloc, try alloc.dupe(u8, "nullable"), .{ .bool = column.nullable });
                try columns.append(.{ .object = column_obj });
            }
            try source_obj.put(alloc, try alloc.dupe(u8, "columns"), .{ .array = columns });
        }
        try root.put(alloc, try alloc.dupe(u8, entry.name), .{ .object = source_obj });
    }
    return .{ .object = root };
}

// -- hit/source merging --

fn findFirstMatchingRightHit(
    join: SupportedJoinRequest,
    left_value: std.json.Value,
    right_hits: []const std.json.Value,
) ?std.json.Value {
    return join_model.findMatchingRightHit(
        right_hits,
        join.right_field,
        left_value,
        join.operator,
        extractJoinValueFromHit,
        struct {
            fn call(operator: metadata_openapi.JoinOperator, left: std.json.Value, right: std.json.Value) bool {
                return jsonValuesCompare(left, right, operator);
            }
        }.call,
    );
}

fn mergeRightHitIntoSource(
    alloc: std.mem.Allocator,
    source_value: *std.json.Value,
    join: SupportedJoinRequest,
    right_hit: std.json.Value,
) !void {
    try join_model.mergeRightHitIntoSourceAlloc(alloc, source_value, join, right_hit);
}

fn removeFieldFromSourceObject(
    alloc: std.mem.Allocator,
    source_value: *std.json.Value,
    field_name: []const u8,
) void {
    join_model.removeFieldFromSourceObject(alloc, source_value, field_name);
}

// -- foreign hit builders --

fn freeOwnedStringSlice(alloc: std.mem.Allocator, values: [][]u8) void {
    for (values) |value| alloc.free(value);
    if (values.len > 0) alloc.free(values);
}

fn buildForeignJoinFieldListAlloc(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
) ![][]u8 {
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |value| alloc.free(value);
        out.deinit(alloc);
    }

    if (join.right_fields.len > 0) {
        for (join.right_fields) |field| {
            if (join.nested_join != null and std.mem.indexOfScalar(u8, field, '.') != null) continue;
            try out.append(alloc, try alloc.dupe(u8, field));
        }
        var has_match_field = false;
        var has_nested_left_field = false;
        for (join.right_fields) |field| {
            if (std.mem.eql(u8, field, join.right_field)) {
                has_match_field = true;
            }
            if (join.nested_join) |nested| {
                if (std.mem.eql(u8, field, nested.left_field)) has_nested_left_field = true;
            }
        }
        if (!has_match_field) try out.append(alloc, try alloc.dupe(u8, join.right_field));
        if (join.nested_join) |nested| {
            if (!has_nested_left_field) try out.append(alloc, try alloc.dupe(u8, nested.left_field));
        }
    }

    return try out.toOwnedSlice(alloc);
}

fn scalarJsonValueStringAlloc(alloc: std.mem.Allocator, value: std.json.Value) !?[]u8 {
    return try json_helpers.scalarJsonValueStringAlloc(alloc, value);
}

fn buildForeignRightJoinHit(
    alloc: std.mem.Allocator,
    foreign_source: foreign_mod.PostgresConfig,
    row: std.json.Value,
    match_value: std.json.Value,
) !std.json.Value {
    var hit_obj = std.json.ObjectMap.empty;
    errdefer {
        var it = hit_obj.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        hit_obj.deinit(alloc);
    }

    if ((try foreign_sources_api.deriveSearchIdAlloc(alloc, foreign_source, row)) orelse try scalarJsonValueStringAlloc(alloc, match_value)) |text| {
        try hit_obj.put(alloc, try alloc.dupe(u8, "_id"), .{ .string = text });
    }
    try hit_obj.put(alloc, try alloc.dupe(u8, "_score"), .{ .float = 0 });
    try hit_obj.put(alloc, try alloc.dupe(u8, "_source"), try cloneJsonValue(alloc, row));
    return .{ .object = hit_obj };
}

// -- group-local query helpers --

fn appendGroupLocalJoinHits(
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_id: u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    explicit_limit: bool,
    hits: *std.json.Array,
) !bool {
    var query_req = req;
    var response = (try source.queryGroupLocal(alloc, group_id, table_name, query_req, .read_index)) orelse return false;
    defer response.deinit(alloc);

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, response.json, .{}) catch return error.InternalFailure;
    defer parsed.deinit();
    var hits_ptr = try queryHitsArrayPtr(&parsed.value);
    const total_hits = try queryTotalHits(parsed.value);

    if (!explicit_limit and hits_ptr.items.len < total_hits) {
        query_req.limit = @intCast(total_hits);
        var full_response = (try source.queryGroupLocal(alloc, group_id, table_name, query_req, .read_index)) orelse return false;
        defer full_response.deinit(alloc);
        parsed.deinit();
        parsed = std.json.parseFromSlice(std.json.Value, alloc, full_response.json, .{}) catch return error.InternalFailure;
        hits_ptr = try queryHitsArrayPtr(&parsed.value);
    }

    for (hits_ptr.items) |hit| {
        try hits.append(try cloneJsonValue(alloc, hit));
    }
    return true;
}

fn appendJoinHitsAcrossGroups(
    alloc: std.mem.Allocator,
    source: table_reads.TableReadSource,
    group_ids: []const u64,
    table_name: []const u8,
    req: db_mod.types.SearchRequest,
    hits: *std.json.Array,
) !?usize {
    var groups_queried: usize = 0;
    for (group_ids) |group_id| {
        groups_queried += 1;
        if (!try appendGroupLocalJoinHits(alloc, source, group_id, table_name, req, false, hits)) return null;
    }
    return groups_queried;
}

fn rightJoinGroupIdsFromSnapshot(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
) ![]u64 {
    var group_ids: std.ArrayList(u64) = .empty;
    errdefer group_ids.deinit(alloc);
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        try group_ids.append(alloc, range.group_id);
    }
    return try group_ids.toOwnedSlice(alloc);
}

fn rightJoinGroupForKey(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
    key: []const u8,
) ?u64 {
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        if (rightJoinRangeContainsKey(range, key)) return range.group_id;
    }
    return null;
}

fn rightJoinRangeContainsKey(range: metadata_table_manager.RangeRecord, key: []const u8) bool {
    if (std.mem.order(u8, key, range.start_key) == .lt) return false;
    if (range.end_key) |end_key| {
        if (std.mem.order(u8, key, end_key) != .lt) return false;
    }
    return true;
}

// -- response metadata helpers --

fn firstResponseObjectPtr(root: *std.json.Value) !*std.json.Value {
    return try join_model.firstResponseObjectPtr(root);
}

fn responseProfileObjectPtr(root: *std.json.Value) ?*std.json.Value {
    return join_model.responseProfileObjectPtr(root);
}

fn responseJoinProfileObjectPtr(root: *std.json.Value) ?*std.json.Value {
    const profile_ptr = responseProfileObjectPtr(root) orelse return null;
    const join_ptr = profile_ptr.object.getPtr("join") orelse return null;
    if (join_ptr.* != .object) return null;
    return join_ptr;
}

fn computeJsonHitMaxScore(hits: []const std.json.Value) f64 {
    return join_model.computeJsonHitMaxScore(hits);
}

fn buildJoinPartitionExecutionResultAlloc(
    alloc: std.mem.Allocator,
    hits: []const std.json.Value,
    stats: JoinedQueryStats,
    groups_queried: usize,
    matched_right_ids: *const std.StringHashMapUnmanaged(void),
    job_id: ?u64,
    job_phase: ?JoinShuffleJobPhase,
    total_partitions: usize,
    completed_partitions: usize,
    expires_at_millis: u64,
    worker_retries: usize,
    worker_attempts: []const JoinPartitionExecutionResult.WorkerAttempt,
) !JoinPartitionExecutionResult {
    const shell = try join_model.cloneJoinShellAlloc(JoinedQueryStats, alloc, hits, stats, matched_right_ids);
    errdefer {
        var owned_shell = shell;
        owned_shell.deinit(alloc);
    }

    const owned_worker_attempts = try alloc.dupe(JoinPartitionExecutionResult.WorkerAttempt, worker_attempts);
    errdefer alloc.free(owned_worker_attempts);

    return joinPartitionExecutionResultFromShell(shell, .{
        .groups_queried = groups_queried,
        .job_id = job_id,
        .job_phase = job_phase,
        .total_partitions = total_partitions,
        .completed_partitions = completed_partitions,
        .expires_at_millis = expires_at_millis,
        .worker_retries = worker_retries,
        .worker_attempts = owned_worker_attempts,
    });
}

fn joinPartitionExecutionResultFromShell(
    shell: JoinedRightMergeResult,
    extra: struct {
        groups_queried: usize = 0,
        job_id: ?u64 = null,
        job_phase: ?JoinShuffleJobPhase = null,
        total_partitions: usize = 0,
        completed_partitions: usize = 0,
        expires_at_millis: u64 = 0,
        worker_retries: usize = 0,
        worker_attempts: []JoinPartitionExecutionResult.WorkerAttempt = &.{},
    },
) JoinPartitionExecutionResult {
    return .{
        .hits = shell.hits,
        .stats = shell.stats,
        .groups_queried = extra.groups_queried,
        .matched_right_ids = shell.matched_right_ids,
        .job_id = extra.job_id,
        .job_phase = extra.job_phase,
        .total_partitions = extra.total_partitions,
        .completed_partitions = extra.completed_partitions,
        .expires_at_millis = extra.expires_at_millis,
        .worker_retries = extra.worker_retries,
        .worker_attempts = extra.worker_attempts,
    };
}

// -- string helpers --

fn containsString(items: []const []const u8, needle: []const u8) bool {
    for (items) |item| {
        if (std.mem.eql(u8, item, needle)) return true;
    }
    return false;
}

fn joinStrategyUsedString(strategy: RightJoinQueryResult.StrategyUsed) []const u8 {
    return switch (strategy) {
        .index_lookup => "index_lookup",
        .broadcast => "broadcast",
        .shuffle => "shuffle",
    };
}

fn joinExecutionModeString(mode: JoinShuffleExecutionMode) []const u8 {
    return switch (mode) {
        .transient => "distributed_transient",
        .durable => "distributed_durable",
    };
}

// -- phase/encode helpers --

pub fn phaseString(phase: JoinShuffleJobPhase) []const u8 {
    return switch (phase) {
        .preparing => "preparing",
        .dispatching => "dispatching",
        .finalizing => "finalizing",
        .succeeded => "succeeded",
        .failed => "failed",
    };
}

pub fn phaseFromString(text: []const u8) !JoinShuffleJobPhase {
    if (std.mem.eql(u8, text, "preparing")) return .preparing;
    if (std.mem.eql(u8, text, "dispatching")) return .dispatching;
    if (std.mem.eql(u8, text, "finalizing")) return .finalizing;
    if (std.mem.eql(u8, text, "succeeded")) return .succeeded;
    if (std.mem.eql(u8, text, "failed")) return .failed;
    return error.InvalidQueryRequest;
}

fn joinJobKey(alloc: std.mem.Allocator, job_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "__api_join_jobs__:{d}", .{job_id});
}

fn putOwnedJsonField(
    alloc: std.mem.Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    try obj.put(alloc, try alloc.dupe(u8, key), value);
}

fn putOwnedJsonU64Field(
    alloc: std.mem.Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
    value: u64,
) !void {
    try putOwnedJsonField(alloc, obj, key, try jsonU64ValueAlloc(alloc, value));
}

fn jsonU64ValueAlloc(
    alloc: std.mem.Allocator,
    value: u64,
) !std.json.Value {
    if (std.math.cast(i64, value)) |signed| {
        return .{ .integer = signed };
    }
    return .{ .number_string = try std.fmt.allocPrint(alloc, "{d}", .{value}) };
}

pub fn encodeSupportedJoinClauseValue(
    alloc: std.mem.Allocator,
    join: SupportedJoinRequest,
) !std.json.Value {
    return try buildSupportedJoinClauseValue(alloc, join);
}

fn jsonObjectInteger(obj: std.json.ObjectMap, key: []const u8) !i64 {
    const value = obj.get(key) orelse return error.InvalidQueryRequest;
    return switch (value) {
        .integer => value.integer,
        else => error.InvalidQueryRequest,
    };
}

// ---------------------------------------------------------------------------
// JSON utility functions
// ---------------------------------------------------------------------------

pub fn stringifyJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try json_helpers.stringifyJsonValueAlloc(alloc, value);
}

pub fn cloneJsonValue(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    return try json_helpers.cloneJsonValue(alloc, value);
}

pub fn deinitJsonValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
    json_helpers.deinitJsonValue(alloc, value);
}

pub fn jsonValuesEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
    return json_helpers.jsonValuesEqual(lhs, rhs);
}

test "distributed join applies auth row filter to right table filter query" {
    const alloc = std.testing.allocator;
    var existing = std.json.parseFromSlice(std.json.Value, alloc, "{\"term\":{\"tier\":\"premium\"}}", .{}) catch unreachable;
    defer existing.deinit();
    var join = SupportedJoinRequest{
        .right_table = try alloc.dupe(u8, "customers"),
        .left_field = try alloc.dupe(u8, "customer_id"),
        .right_field = try alloc.dupe(u8, "id"),
        .right_filters = .{
            .filter_query = try cloneJsonValue(alloc, existing.value),
        },
    };
    defer join.deinit(alloc);

    try applyRightTableRowFilterJson(alloc, &join, "{\"term\":{\"tenant_id\":\"acme\"}}");
    const filter_query = join.right_filters.?.filter_query orelse return error.TestExpectedEqual;
    const json = try stringifyJsonValueAlloc(alloc, filter_query);
    defer alloc.free(json);

    try std.testing.expect(std.mem.indexOf(u8, json, "\"conjuncts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"tier\":\"premium\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"tenant_id\":\"acme\"") != null);
}

/// Ordered comparison of two JSON values. Returns -1 (less), 0 (equal), or
/// 1 (greater). Handles cross-type integer/float coercion to match Go's
/// evaluator.CompareOrdered semantics.
pub fn jsonValuesOrdered(lhs: std.json.Value, rhs: std.json.Value) i8 {
    // Numeric types (integer and float) with cross-type coercion.
    const lhs_f64 = jsonNumericToF64(lhs);
    const rhs_f64 = jsonNumericToF64(rhs);
    if (lhs_f64) |l| {
        if (rhs_f64) |r| return orderF64(l, r);
    }

    // String vs string (lexicographic).
    if (lhs == .string and rhs == .string) {
        return orderSlice(lhs.string, rhs.string);
    }

    // number_string vs number_string (lexicographic).
    if (lhs == .number_string and rhs == .number_string) {
        return orderSlice(lhs.number_string, rhs.number_string);
    }

    // Incomparable types — treat as equal (consistent with Go returning 0 for
    // unhandled types).
    return 0;
}

fn jsonNumericToF64(v: std.json.Value) ?f64 {
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |f| f,
        else => null,
    };
}

fn orderF64(l: f64, r: f64) i8 {
    if (l < r) return -1;
    if (l > r) return 1;
    return 0;
}

fn orderSlice(l: []const u8, r: []const u8) i8 {
    return switch (std.mem.order(u8, l, r)) {
        .lt => -1,
        .gt => 1,
        .eq => 0,
    };
}

/// Operator-aware comparison of two JSON values using a JoinOperator.
pub fn jsonValuesCompare(
    lhs: std.json.Value,
    rhs: std.json.Value,
    operator: metadata_openapi.JoinOperator,
) bool {
    return switch (operator) {
        .eq => jsonValuesEqual(lhs, rhs),
        .neq => !jsonValuesEqual(lhs, rhs),
        .lt => jsonValuesOrdered(lhs, rhs) < 0,
        .lte => jsonValuesOrdered(lhs, rhs) <= 0,
        .gt => jsonValuesOrdered(lhs, rhs) > 0,
        .gte => jsonValuesOrdered(lhs, rhs) >= 0,
    };
}

// ---------------------------------------------------------------------------
// File-level helpers (originally at file scope in http_server.zig)
// ---------------------------------------------------------------------------

pub fn cloneFieldList(alloc: std.mem.Allocator, raw_fields: []const []const u8) ![][]const u8 {
    const fields = try alloc.alloc([]const u8, raw_fields.len);
    var field_index: usize = 0;
    errdefer {
        for (fields[0..field_index]) |field| alloc.free(field);
        alloc.free(fields);
    }
    for (raw_fields) |field| {
        fields[field_index] = try alloc.dupe(u8, field);
        field_index += 1;
    }
    return fields;
}

pub fn freeFieldList(alloc: std.mem.Allocator, fields: [][]const u8) void {
    for (fields) |field| alloc.free(field);
    if (fields.len > 0) alloc.free(fields);
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub fn joinJobNowMillis() u64 {
    return @divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms);
}

pub fn preferredFinalizerStartIndex(job_id: u64, worker_group_ids: []const u64) usize {
    if (worker_group_ids.len == 0) return 0;
    return @intCast(job_id % worker_group_ids.len);
}

fn indexOfWorkerGroup(worker_group_ids: []const u64, group_id: u64) ?usize {
    for (worker_group_ids, 0..) |candidate, index| {
        if (candidate == group_id) return index;
    }
    return null;
}

test "distributed join buildJoinPartitionExecutionResultAlloc clones owned inputs" {
    const alloc = std.testing.allocator;

    const original_hits = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(original_hits);
    original_hits[0] = try testJoinHitAlloc(alloc, "doc:1", 3.5, "1");

    var matched_right_ids = std.StringHashMapUnmanaged(void){};
    defer matched_right_ids.deinit(alloc);
    try matched_right_ids.put(alloc, "right:1", {});

    const worker_attempts = [_]JoinPartitionExecutionResult.WorkerAttempt{
        .{
            .partition_index = 2,
            .worker_group_id = 42,
            .succeeded = true,
        },
    };

    var result = try buildJoinPartitionExecutionResultAlloc(
        alloc,
        original_hits,
        .{
            .left_rows_scanned = 1,
            .right_rows_scanned = 2,
            .rows_matched = 1,
        },
        3,
        &matched_right_ids,
        123,
        .dispatching,
        4,
        2,
        999,
        1,
        &worker_attempts,
    );
    defer result.deinit(alloc);

    json_helpers.deinitJsonValue(alloc, &original_hits[0]);
    alloc.free(original_hits);

    try std.testing.expectEqual(@as(usize, 1), result.hits.len);
    try std.testing.expectEqualStrings("doc:1", result.hits[0].object.get("_id").?.string);
    try std.testing.expectEqual(@as(usize, 1), result.matched_right_ids.len);
    try std.testing.expectEqualStrings("right:1", result.matched_right_ids[0]);
    try std.testing.expectEqual(@as(usize, 1), result.worker_attempts.len);
    try std.testing.expectEqual(@as(usize, 2), result.worker_attempts[0].partition_index);
    try std.testing.expectEqual(@as(u64, 42), result.worker_attempts[0].worker_group_id);
    try std.testing.expectEqual(@as(?u64, 123), result.job_id);
    try std.testing.expectEqual(@as(?JoinShuffleJobPhase, .dispatching), result.job_phase);
    try std.testing.expectEqual(@as(usize, 4), result.total_partitions);
    try std.testing.expectEqual(@as(usize, 2), result.completed_partitions);
}

test "distributed right join unmatched completion appends hits and updates stats" {
    const alloc = std.testing.allocator;

    const existing_hits = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(existing_hits);
    existing_hits[0] = try testJoinHitAlloc(alloc, "doc:left", 1.0, "left");

    var result = JoinPartitionExecutionResult{
        .hits = existing_hits,
        .stats = .{
            .left_rows_scanned = 1,
            .right_rows_scanned = 2,
            .rows_matched = 1,
        },
        .groups_queried = 1,
    };
    defer result.deinit(alloc);

    var join = try testSupportedJoinRequestAlloc(alloc);
    defer join.deinit(alloc);
    join.join_type = .right;

    const right_hits = try alloc.alloc(std.json.Value, 2);
    defer {
        for (right_hits) |*item| deinitJsonValue(alloc, item);
        alloc.free(right_hits);
    }
    right_hits[0] = try testJoinHitAlloc(alloc, "doc:matched", 0.0, "matched");
    right_hits[1] = try testJoinHitAlloc(alloc, "doc:right", 0.0, "right");

    var matched_right_ids = std.StringHashMapUnmanaged(void){};
    defer matched_right_ids.deinit(alloc);
    try matched_right_ids.put(alloc, "doc:matched", {});

    const completion_hits = try buildDistributedRightJoinUnmatchedHitsAlloc(
        alloc,
        right_hits,
        join,
        &.{},
        false,
        &matched_right_ids,
    );
    var completion: DistributedRightJoinUnmatchedCompletion = .{
        .hits = completion_hits,
        .groups_queried = 4,
        .right_rows_scanned = right_hits.len,
    };
    defer completion.deinit(alloc);
    try appendBuiltDistributedRightJoinUnmatchedHitsToResultAlloc(alloc, &result, &completion);

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqualStrings("doc:left", result.hits[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("doc:right", result.hits[1].object.get("_id").?.string);
    try std.testing.expectEqual(@as(i64, 4), result.stats.right_rows_scanned);
    try std.testing.expectEqual(@as(i64, 1), result.stats.rows_unmatched_right);
    try std.testing.expectEqual(@as(usize, 4), result.groups_queried);
}

test "distributed join unmatched worker returns only unmatched synthetic hits" {
    const FakeCtx = struct {
        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return null;
        }
        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn getJoinShuffleLease(_: *anyopaque, _: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            return null;
        }
        fn upsertJoinShuffleLease(_: *anyopaque, _: metadata_table_manager.ShuffleJoinLeaseRecord) !void {}
        fn removeJoinShuffleLease(_: *anyopaque, _: u64) !void {}
        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }
        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }
        fn buildOwnedSearchRequest(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query_value: std.json.Value) !query_api.OwnedQueryRequest {
            const body = try stringifyJsonValueAlloc(alloc, query_value);
            defer alloc.free(body);
            return try query_api.parseQueryRequest(alloc, null, table_name, body);
        }
        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }

        fn ctx(self: *@This()) JoinContext {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .get_join_shuffle_lease = getJoinShuffleLease,
                    .upsert_join_shuffle_lease = upsertJoinShuffleLease,
                    .remove_join_shuffle_lease = removeJoinShuffleLease,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
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

        fn queryGroupLocal(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            try std.testing.expectEqual(@as(u64, 201), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            return .{ .json = try alloc.dupe(u8, "{\"responses\":[{\"hits\":{\"total\":2,\"max_score\":0,\"hits\":[{\"_id\":\"cust:a\",\"_score\":0,\"_source\":{\"name\":\"Alice\"}},{\"_id\":\"cust:z\",\"_score\":0,\"_source\":{\"name\":\"Zoe\"}}]}}]}") };
        }
    };

    const alloc = std.testing.allocator;
    var ctx_source = FakeCtx{};
    var reads = FakeReads{};

    var join = try testSupportedJoinRequestAlloc(alloc);
    defer join.deinit(alloc);
    join.join_type = .right;

    const left_hits = try alloc.alloc(std.json.Value, 1);
    defer {
        for (left_hits) |*item| deinitJsonValue(alloc, item);
        alloc.free(left_hits);
    }
    left_hits[0] = try testJoinHitAlloc(alloc, "doc:left", 1.0, "cust:a");

    const left_fields = [_][]const u8{"title"};
    const body = try encodeJoinUnmatchedRequest(alloc, join, left_hits.len, &left_fields, false, &.{"cust:a"});
    defer alloc.free(body);

    const response = try executeJoinUnmatchedLocal(ctx_source.ctx(), alloc, reads.source(), 201, "customers", body);
    defer {
        for (@constCast(response.hits)) |*item| deinitJsonValue(alloc, item);
        if (response.hits.len > 0) alloc.free(response.hits);
    }

    try std.testing.expectEqual(@as(usize, 1), response.hits.len);
    try std.testing.expectEqual(@as(u64, 2), response.right_rows_scanned);
    try std.testing.expectEqualStrings("cust:z", response.hits[0].object.get("_id").?.string);
    try std.testing.expect(extractJoinValueFromHit(response.hits[0], "title").? == .null);
    const customer_name = extractJoinValueFromHit(response.hits[0], "customers.name").?;
    try std.testing.expectEqualStrings("Zoe", customer_name.string);
}

test "distributed join unmatched worker pages group-local right hits" {
    const FakeCtx = struct {
        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return null;
        }
        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn getJoinShuffleLease(_: *anyopaque, _: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            return null;
        }
        fn upsertJoinShuffleLease(_: *anyopaque, _: metadata_table_manager.ShuffleJoinLeaseRecord) !void {}
        fn removeJoinShuffleLease(_: *anyopaque, _: u64) !void {}
        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }
        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }
        fn buildOwnedSearchRequest(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query_value: std.json.Value) !query_api.OwnedQueryRequest {
            const body = try json_helpers.stringifyJsonValueAlloc(alloc, query_value);
            defer alloc.free(body);
            return try query_api.parseQueryRequest(alloc, null, table_name, body);
        }
        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }

        fn ctx(self: *@This()) JoinContext {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .get_join_shuffle_lease = getJoinShuffleLease,
                    .upsert_join_shuffle_lease = upsertJoinShuffleLease,
                    .remove_join_shuffle_lease = removeJoinShuffleLease,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
        }
    };

    const FakeReads = struct {
        calls: usize = 0,

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

        fn buildPageJsonAlloc(alloc: std.mem.Allocator, start: usize, end: usize, total_hits: usize) ![]u8 {
            var root = std.json.Value{ .object = std.json.ObjectMap.empty };
            defer json_helpers.deinitJsonValue(alloc, &root);

            var responses = std.json.Array.init(alloc);
            var response = std.json.Value{ .object = std.json.ObjectMap.empty };
            var hits_obj = std.json.Value{ .object = std.json.ObjectMap.empty };
            var hits = std.json.Array.init(alloc);

            for (start..end) |i| {
                const id = try std.fmt.allocPrint(alloc, "cust:{d}", .{i});
                errdefer alloc.free(id);
                const name = try std.fmt.allocPrint(alloc, "User {d}", .{i});
                errdefer alloc.free(name);

                var source_obj = std.json.ObjectMap.empty;
                try source_obj.put(alloc, try alloc.dupe(u8, "name"), .{ .string = name });

                var hit_obj = std.json.ObjectMap.empty;
                try hit_obj.put(alloc, try alloc.dupe(u8, "_id"), .{ .string = id });
                try hit_obj.put(alloc, try alloc.dupe(u8, "_score"), .{ .float = 0 });
                try hit_obj.put(alloc, try alloc.dupe(u8, "_source"), .{ .object = source_obj });
                try hits.append(.{ .object = hit_obj });
            }

            try hits_obj.object.put(alloc, try alloc.dupe(u8, "total"), .{ .integer = @intCast(total_hits) });
            try hits_obj.object.put(alloc, try alloc.dupe(u8, "max_score"), .{ .float = 0 });
            try hits_obj.object.put(alloc, try alloc.dupe(u8, "hits"), .{ .array = hits });
            try response.object.put(alloc, try alloc.dupe(u8, "hits"), hits_obj);
            try responses.append(response);
            try root.object.put(alloc, try alloc.dupe(u8, "responses"), .{ .array = responses });
            return try json_helpers.stringifyJsonValueAlloc(alloc, root);
        }

        fn queryGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            var self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 201), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            self.calls += 1;

            const total_hits: usize = unmatched_right_join_group_chunk_limit + 2;
            const start: usize = req.offset;
            const end: usize = @min(total_hits, start + req.limit);
            return .{ .json = try buildPageJsonAlloc(alloc, start, end, total_hits) };
        }
    };

    const alloc = std.testing.allocator;
    var ctx_source = FakeCtx{};
    var reads = FakeReads{};

    var join = try testSupportedJoinRequestAlloc(alloc);
    defer join.deinit(alloc);
    join.join_type = .right;

    const left_fields = [_][]const u8{"title"};
    const body = try encodeJoinUnmatchedRequest(alloc, join, 1, &left_fields, false, &.{"cust:0"});
    defer alloc.free(body);

    const response = try executeJoinUnmatchedLocal(ctx_source.ctx(), alloc, reads.source(), 201, "customers", body);
    defer {
        for (@constCast(response.hits)) |*item| json_helpers.deinitJsonValue(alloc, item);
        if (response.hits.len > 0) alloc.free(response.hits);
    }

    try std.testing.expect(reads.calls >= 2);
    try std.testing.expectEqual(@as(u64, unmatched_right_join_group_chunk_limit + 2), response.right_rows_scanned);
    try std.testing.expectEqual(@as(usize, unmatched_right_join_group_chunk_limit + 1), response.hits.len);
    try std.testing.expectEqualStrings("cust:1", response.hits[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings(
        "User 129",
        extractJoinValueFromHit(response.hits[response.hits.len - 1], "customers.name").?.string,
    );
}

test "distributed join unmatched worker prefers local search results over query envelopes" {
    const FakeCtx = struct {
        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return null;
        }
        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn getJoinShuffleLease(_: *anyopaque, _: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            return null;
        }
        fn upsertJoinShuffleLease(_: *anyopaque, _: metadata_table_manager.ShuffleJoinLeaseRecord) !void {}
        fn removeJoinShuffleLease(_: *anyopaque, _: u64) !void {}
        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }
        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }
        fn buildOwnedSearchRequest(_: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, query_value: std.json.Value) !query_api.OwnedQueryRequest {
            const body = try json_helpers.stringifyJsonValueAlloc(alloc, query_value);
            defer alloc.free(body);
            return try query_api.parseQueryRequest(alloc, null, table_name, body);
        }
        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }

        fn ctx(self: *@This()) JoinContext {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .get_join_shuffle_lease = getJoinShuffleLease,
                    .upsert_join_shuffle_lease = upsertJoinShuffleLease,
                    .remove_join_shuffle_lease = removeJoinShuffleLease,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
        }
    };

    const FakeReads = struct {
        calls: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
                    .query_group_local = queryGroupLocal,
                    .search_result_group_local = searchResultGroupLocal,
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

        fn queryGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?query_api.QueryResponse {
            return error.TestUnexpectedResult;
        }

        fn searchResultGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?db_mod.types.SearchResult {
            var self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 201), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            self.calls += 1;

            const total_hits: usize = unmatched_right_join_group_chunk_limit + 2;
            const start: usize = req.offset;
            const end: usize = @min(total_hits, start + req.limit);
            const hits = try alloc.alloc(db_mod.types.SearchHit, end - start);
            errdefer alloc.free(hits);
            for (start..end, 0..) |i, out_index| {
                hits[out_index] = .{
                    .id = try std.fmt.allocPrint(alloc, "cust:{d}", .{i}),
                    .score = 0,
                    .stored_data = try std.fmt.allocPrint(alloc, "{{\"name\":\"User {d}\"}}", .{i}),
                };
            }
            return .{
                .alloc = alloc,
                .hits = hits,
                .total_hits = @intCast(total_hits),
            };
        }
    };

    const alloc = std.testing.allocator;
    var ctx_source = FakeCtx{};
    var reads = FakeReads{};

    var join = try testSupportedJoinRequestAlloc(alloc);
    defer join.deinit(alloc);
    join.join_type = .right;

    const left_fields = [_][]const u8{"title"};
    const body = try encodeJoinUnmatchedRequest(alloc, join, 1, &left_fields, false, &.{"cust:0"});
    defer alloc.free(body);

    const response = try executeJoinUnmatchedLocal(ctx_source.ctx(), alloc, reads.source(), 201, "customers", body);
    defer {
        for (@constCast(response.hits)) |*item| json_helpers.deinitJsonValue(alloc, item);
        if (response.hits.len > 0) alloc.free(response.hits);
    }

    try std.testing.expect(reads.calls >= 2);
    try std.testing.expectEqual(@as(u64, unmatched_right_join_group_chunk_limit + 2), response.right_rows_scanned);
    try std.testing.expectEqual(@as(usize, unmatched_right_join_group_chunk_limit + 1), response.hits.len);
    try std.testing.expectEqualStrings("cust:1", response.hits[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings(
        "User 129",
        extractJoinValueFromHit(response.hits[response.hits.len - 1], "customers.name").?.string,
    );
}

test "distributed join partition state restores applies worker result and builds result" {
    const alloc = std.testing.allocator;

    var state = StatefulShufflePartitionState.init(alloc);
    defer state.deinit(alloc);

    const resume_hits = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(resume_hits);
    resume_hits[0] = try testJoinHitAlloc(alloc, "doc:resume", 1.25, "resume");

    const resume_ids = try alloc.alloc([]u8, 1);
    errdefer alloc.free(resume_ids);
    resume_ids[0] = try alloc.dupe(u8, "right:resume");

    const resume_attempts = try alloc.alloc(JoinPartitionExecutionResult.WorkerAttempt, 1);
    errdefer alloc.free(resume_attempts);
    resume_attempts[0] = .{
        .partition_index = 0,
        .worker_group_id = 11,
        .succeeded = true,
    };

    var resume_state = JoinShuffleResumeState{
        .result = .{
            .hits = resume_hits,
            .stats = .{
                .left_rows_scanned = 2,
                .right_rows_scanned = 4,
                .rows_matched = 1,
            },
            .matched_right_ids = resume_ids,
            .completed_partitions = 1,
            .worker_retries = 2,
            .worker_attempts = resume_attempts,
        },
        .next_partition_index = 1,
    };
    defer resume_state.deinit(alloc);

    try state.restoreAlloc(alloc, resume_state);
    try std.testing.expectEqual(@as(usize, 1), state.joined_hits.items.len);
    try std.testing.expectEqual(@as(usize, 1), state.groupsQueried());
    try std.testing.expectEqual(@as(usize, 1), state.next_partition_index);
    try std.testing.expectEqual(@as(usize, 2), state.worker_retries);

    const worker_hits = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(worker_hits);
    worker_hits[0] = try testJoinHitAlloc(alloc, "doc:new", 3.5, "new");

    const worker_ids = try alloc.alloc([]u8, 1);
    errdefer alloc.free(worker_ids);
    worker_ids[0] = try alloc.dupe(u8, "right:new");

    var worker_result = JoinPartitionExecutionResult{
        .hits = worker_hits,
        .stats = .{
            .left_rows_scanned = 3,
            .right_rows_scanned = 6,
            .rows_matched = 2,
            .rows_unmatched_left = 1,
        },
        .matched_right_ids = worker_ids,
    };
    defer worker_result.deinit(alloc);

    try state.applyWorkerResultAlloc(alloc, &worker_result);
    try state.worker_attempts.append(alloc, .{
        .partition_index = 1,
        .worker_group_id = 77,
        .succeeded = true,
    });
    try state.seen_groups.put(alloc, 77, {});
    state.markPartitionCompleted(1);

    var job_store = JoinJobStore.init(alloc, .{});
    defer job_store.deinit();

    var result = try state.buildResultAlloc(alloc, &job_store, null, 3);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqual(@as(i64, 5), result.stats.left_rows_scanned);
    try std.testing.expectEqual(@as(i64, 10), result.stats.right_rows_scanned);
    try std.testing.expectEqual(@as(i64, 3), result.stats.rows_matched);
    try std.testing.expectEqual(@as(i64, 1), result.stats.rows_unmatched_left);
    try std.testing.expectEqual(@as(usize, 2), result.groups_queried);
    try std.testing.expectEqual(@as(usize, 2), result.worker_retries);
    try std.testing.expectEqual(@as(usize, 2), result.completed_partitions);
    try std.testing.expectEqual(@as(usize, 2), result.worker_attempts.len);
    try std.testing.expect(testContainsString(result.matched_right_ids, "right:resume"));
    try std.testing.expect(testContainsString(result.matched_right_ids, "right:new"));
}

test "distributed join finalizer state applies attempts and metadata" {
    const alloc = std.testing.allocator;

    var finalizer = StatefulShuffleFinalizerState{
        .stable_job_id = 41,
        .durable = false,
        .durable_job_id = 41,
        .previous_owner_group_id = 7,
        .execution_mode = .durable,
    };
    defer finalizer.deinit(alloc);

    try finalizer.recordAttempt(alloc, 7, false);
    try finalizer.recordAttempt(alloc, 9, true);

    const old_attempts = try alloc.alloc(JoinPartitionExecutionResult.FinalizerAttempt, 1);
    errdefer alloc.free(old_attempts);
    old_attempts[0] = .{ .worker_group_id = 1, .succeeded = false };

    var result = JoinPartitionExecutionResult{
        .hits = &.{},
        .finalizer_retries = 1,
        .finalizer_attempts = old_attempts,
    };
    defer result.deinit(alloc);

    var job_store = JoinJobStore.init(alloc, .{});
    defer job_store.deinit();

    try finalizer.finalizeResult(alloc, &job_store, &result, 9, 2, true);

    try std.testing.expectEqual(@as(?u64, 41), result.job_id);
    try std.testing.expectEqual(@as(?u64, 9), result.finalizer_group_id);
    try std.testing.expectEqual(@as(usize, 3), result.finalizer_retries);
    try std.testing.expectEqual(JoinShuffleExecutionMode.durable, result.execution_mode);
    try std.testing.expect(result.coordinator_finalized);
    try std.testing.expectEqual(@as(usize, 2), result.finalizer_attempts.len);
    try std.testing.expectEqual(@as(u64, 7), result.finalizer_attempts[0].worker_group_id);
    try std.testing.expect(!result.finalizer_attempts[0].succeeded);
    try std.testing.expectEqual(@as(u64, 9), result.finalizer_attempts[1].worker_group_id);
    try std.testing.expect(result.finalizer_attempts[1].succeeded);
    try std.testing.expectEqual(@as(?u64, 7), finalizer.handoffOwnerGroupId(9));
    try std.testing.expectEqual(@as(?u64, null), finalizer.handoffOwnerGroupId(7));
}

fn testSupportedJoinRequestAlloc(alloc: std.mem.Allocator) !SupportedJoinRequest {
    return .{
        .right_table = try alloc.dupe(u8, "customers"),
        .join_type = .inner,
        .left_field = try alloc.dupe(u8, "customer_id"),
        .right_field = try alloc.dupe(u8, "_id"),
    };
}

test "distributed join lifecycle prepare returns fresh and records start when no durable state exists" {
    const alloc = std.testing.allocator;

    var job_store = JoinJobStore.init(alloc, .{});
    defer job_store.deinit();

    const lifecycle: StatefulShuffleJobLifecycle = .{
        .job_store = &job_store,
        .alloc = alloc,
        .source = undefined,
        .table_name = "customers",
        .finalizer_group_id = 17,
        .job_id = 101,
    };

    var join = try testSupportedJoinRequestAlloc(alloc);
    defer join.deinit(alloc);

    var prepared = try lifecycle.prepareAlloc(join, 4, null);
    defer prepared.deinit(alloc);

    switch (prepared) {
        .fresh => {},
        else => return error.TestUnexpectedResult,
    }

    const snapshot = (try job_store.loadJoinJobStateSnapshot(alloc, 101)).?;
    defer alloc.free(snapshot);
    try std.testing.expect(std.mem.indexOf(u8, snapshot, "\"phase\":\"dispatching\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, snapshot, "\"owner_group_id\":17") != null);
}

test "distributed join lifecycle prepare reuses persisted resume state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/lifecycle-resume-state.txt", .{tmp.sub_path});
    defer alloc.free(store_path);

    var job_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
    defer job_store.deinit();

    try job_store.recordJoinJobStart(102, 21, 3);

    const hits = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(hits);
    hits[0] = try testJoinHitAlloc(alloc, "doc:resume", 1.0, "resume");

    var partial = JoinPartitionExecutionResult{
        .hits = hits,
        .stats = .{
            .left_rows_scanned = 2,
            .right_rows_scanned = 4,
            .rows_matched = 1,
        },
        .job_id = 102,
        .job_phase = .finalizing,
        .total_partitions = 3,
        .completed_partitions = 1,
    };
    defer partial.deinit(alloc);
    try job_store.recordJoinJobProgress(102, 2, partial);

    const lifecycle: StatefulShuffleJobLifecycle = .{
        .job_store = &job_store,
        .alloc = alloc,
        .source = undefined,
        .table_name = "customers",
        .finalizer_group_id = 21,
        .job_id = 102,
    };

    var join = try testSupportedJoinRequestAlloc(alloc);
    defer join.deinit(alloc);

    var prepared = try lifecycle.prepareAlloc(join, 3, null);
    defer prepared.deinit(alloc);

    switch (prepared) {
        .resume_state => |resume_state| {
            try std.testing.expectEqual(@as(usize, 2), resume_state.next_partition_index);
            try std.testing.expectEqual(@as(usize, 1), resume_state.result.hits.len);
            try std.testing.expectEqualStrings("doc:resume", resume_state.result.hits[0].object.get("_id").?.string);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "distributed join lifecycle prepare reuses persisted cached result" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/lifecycle-cached-result.txt", .{tmp.sub_path});
    defer alloc.free(store_path);

    var job_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
    defer job_store.deinit();

    try job_store.recordJoinJobStart(103, 31, 2);

    const hits = try alloc.alloc(std.json.Value, 1);
    errdefer alloc.free(hits);
    hits[0] = try testJoinHitAlloc(alloc, "doc:cached", 2.0, "cached");

    var completed = JoinPartitionExecutionResult{
        .hits = hits,
        .stats = .{
            .left_rows_scanned = 3,
            .right_rows_scanned = 5,
            .rows_matched = 1,
        },
        .job_id = 103,
        .job_phase = .succeeded,
        .total_partitions = 2,
        .completed_partitions = 2,
    };
    defer completed.deinit(alloc);

    const encoded = try job_store.encodeJoinPartitionResponse(alloc, completed);
    defer alloc.free(encoded);
    try job_store.recordJoinJobSucceeded(103, 31, 0, false, encoded);

    const lifecycle: StatefulShuffleJobLifecycle = .{
        .job_store = &job_store,
        .alloc = alloc,
        .source = undefined,
        .table_name = "customers",
        .finalizer_group_id = 31,
        .job_id = 103,
    };

    var join = try testSupportedJoinRequestAlloc(alloc);
    defer join.deinit(alloc);

    var prepared = try lifecycle.prepareAlloc(join, 2, null);
    defer prepared.deinit(alloc);

    switch (prepared) {
        .cached_result => |result| {
            try std.testing.expectEqual(@as(?u64, 103), result.job_id);
            try std.testing.expectEqual(@as(?JoinShuffleJobPhase, .succeeded), result.job_phase);
            try std.testing.expectEqual(@as(usize, 1), result.hits.len);
            try std.testing.expectEqualStrings("doc:cached", result.hits[0].object.get("_id").?.string);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "distributed join chooses stateful shuffle or forced broadcast from stats" {
    const left_join = SupportedJoinRequest{
        .right_table = @constCast("right"),
        .join_type = .left,
        .left_field = @constCast("left_id"),
        .right_field = @constCast("right_id"),
    };
    const right_join = SupportedJoinRequest{
        .right_table = @constCast("right"),
        .join_type = .right,
        .left_field = @constCast("left_id"),
        .right_field = @constCast("right_id"),
    };

    const left_decision = chooseStatefulJoinStrategyWithStats(
        left_join,
        false,
        50_000,
        50_000,
        .{ .row_count = 50_000, .size_bytes = join_model.join_broadcast_threshold_bytes * 2, .has_stats = true },
        .{ .row_count = 50_000, .size_bytes = join_model.join_broadcast_threshold_bytes * 2, .has_stats = true },
    );
    try std.testing.expectEqual(RightJoinQueryResult.StrategyUsed.shuffle, left_decision.strategy);
    try std.testing.expect(left_decision.shuffle_candidate);
    try std.testing.expect(!left_decision.forced_broadcast_fallback);
    try std.testing.expect(left_decision.shuffle_partitions > 0);

    const right_decision = chooseStatefulJoinStrategyWithStats(
        right_join,
        false,
        50_000,
        50_000,
        .{ .row_count = 50_000, .size_bytes = join_model.join_broadcast_threshold_bytes * 2, .has_stats = true },
        .{ .row_count = 50_000, .size_bytes = join_model.join_broadcast_threshold_bytes * 2, .has_stats = true },
    );
    try std.testing.expectEqual(RightJoinQueryResult.StrategyUsed.broadcast, right_decision.strategy);
    try std.testing.expect(right_decision.shuffle_candidate);
    try std.testing.expect(right_decision.forced_broadcast_fallback);
    try std.testing.expectEqual(@as(usize, 0), right_decision.shuffle_partitions);

    const lookup_decision = chooseStatefulJoinStrategyWithStats(
        left_join,
        true,
        50_000,
        100,
        .{ .row_count = 50_000, .size_bytes = join_model.join_broadcast_threshold_bytes * 2, .has_stats = true },
        .{ .row_count = 100_000, .size_bytes = join_model.join_broadcast_threshold_bytes * 2, .has_stats = true },
    );
    try std.testing.expectEqual(RightJoinQueryResult.StrategyUsed.index_lookup, lookup_decision.strategy);
    try std.testing.expect(!lookup_decision.shuffle_candidate);
    try std.testing.expect(!lookup_decision.forced_broadcast_fallback);
}

test "distributed join stable job id is deterministic and changes with inputs" {
    const alloc = std.testing.allocator;

    var job_store = JoinJobStore.init(alloc, .{});
    defer job_store.deinit();

    const hits_a = try alloc.alloc(std.json.Value, 2);
    defer {
        for (hits_a) |*hit| json_helpers.deinitJsonValue(alloc, hit);
        alloc.free(hits_a);
    }
    hits_a[0] = try testJoinHitAlloc(alloc, "doc:1", 1.0, "1");
    hits_a[1] = try testJoinHitAlloc(alloc, "doc:2", 2.0, "2");

    const hits_b = try alloc.alloc(std.json.Value, 2);
    defer {
        for (hits_b) |*hit| json_helpers.deinitJsonValue(alloc, hit);
        alloc.free(hits_b);
    }
    hits_b[0] = try testJoinHitAlloc(alloc, "doc:1", 9.0, "1");
    hits_b[1] = try testJoinHitAlloc(alloc, "doc:2", 8.0, "2");

    const join = SupportedJoinRequest{
        .right_table = @constCast("right_table"),
        .join_type = .left,
        .left_field = @constCast("left_id"),
        .right_field = @constCast("_id"),
    };

    const first = try job_store.stableDistributedJoinJobId(alloc, join, hits_a, false, 8);
    const second = try job_store.stableDistributedJoinJobId(alloc, join, hits_b, false, 8);
    const appended_variant = try job_store.stableDistributedJoinJobId(alloc, join, hits_a, true, 8);
    const partition_variant = try job_store.stableDistributedJoinJobId(alloc, join, hits_a, false, 16);

    try std.testing.expect(first != 0);
    try std.testing.expectEqual(first, second);
    try std.testing.expect(first != appended_variant);
    try std.testing.expect(first != partition_variant);
}

test "distributed join job expiry uses lease ttl for active phases and retention for terminal phases" {
    const alloc = std.testing.allocator;

    var job_store = JoinJobStore.init(alloc, .{
        .join_job_lease_ttl_ms = 1_000,
        .join_job_retention_ms = 9_000,
    });
    defer job_store.deinit();

    try std.testing.expectEqual(@as(u64, 6_000), job_store.joinJobExpiryForPhase(.preparing, 5_000));
    try std.testing.expectEqual(@as(u64, 6_000), job_store.joinJobExpiryForPhase(.dispatching, 5_000));
    try std.testing.expectEqual(@as(u64, 6_000), job_store.joinJobExpiryForPhase(.finalizing, 5_000));
    try std.testing.expectEqual(@as(u64, 14_000), job_store.joinJobExpiryForPhase(.succeeded, 5_000));
    try std.testing.expectEqual(@as(u64, 14_000), job_store.joinJobExpiryForPhase(.failed, 5_000));
}

test "distributed join shared finalizer start index prefers live lease owner and falls back deterministically" {
    const FakeLeaseSource = struct {
        lease: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,
        last_upsert: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,

        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return null;
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

        fn removeJoinShuffleLease(_: *anyopaque, _: u64) !void {}
        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }
        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }
        fn buildOwnedSearchRequest(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: std.json.Value) !query_api.OwnedQueryRequest {
            return error.UnsupportedOperation;
        }
        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }

        fn ctx(self: *@This()) JoinContext {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .get_join_shuffle_lease = getJoinShuffleLease,
                    .upsert_join_shuffle_lease = upsertJoinShuffleLease,
                    .remove_join_shuffle_lease = removeJoinShuffleLease,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
        }
    };

    const alloc = std.testing.allocator;
    var job_store = JoinJobStore.init(alloc, .{});
    defer job_store.deinit();

    var source = FakeLeaseSource{
        .lease = .{
            .job_id = 41,
            .owner_group_id = 13,
            .expires_at_ms = std.math.maxInt(u64),
        },
    };
    job_store.setContext(source.ctx());

    const worker_group_ids = [_]u64{ 11, 13, 17 };
    const preferred = job_store.sharedJoinShuffleFinalizerStartIndex(41, &worker_group_ids);
    try std.testing.expectEqual(@as(usize, 1), preferred);
    try std.testing.expectEqual(@as(?u64, 13), source.last_upsert.?.owner_group_id);

    source.lease = .{
        .job_id = 41,
        .owner_group_id = 13,
        .expires_at_ms = 1,
    };
    source.last_upsert = null;
    const fallback = job_store.sharedJoinShuffleFinalizerStartIndex(41, &worker_group_ids);
    const deterministic = preferredFinalizerStartIndex(41, &worker_group_ids);
    try std.testing.expectEqual(deterministic, fallback);
    try std.testing.expectEqual(worker_group_ids[deterministic], source.last_upsert.?.owner_group_id);
}

test "distributed join durable finalizer state init reuses prior owner lease" {
    const FakeLeaseSource = struct {
        lease: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,

        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return null;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn getJoinShuffleLease(ptr: *anyopaque, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const lease = self.lease orelse return null;
            if (lease.job_id != job_id) return null;
            return lease;
        }

        fn upsertJoinShuffleLease(_: *anyopaque, _: metadata_table_manager.ShuffleJoinLeaseRecord) !void {}
        fn removeJoinShuffleLease(_: *anyopaque, _: u64) !void {}
        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }
        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }
        fn buildOwnedSearchRequest(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: std.json.Value) !query_api.OwnedQueryRequest {
            return error.UnsupportedOperation;
        }
        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }

        fn ctx(self: *@This()) JoinContext {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .get_join_shuffle_lease = getJoinShuffleLease,
                    .upsert_join_shuffle_lease = upsertJoinShuffleLease,
                    .remove_join_shuffle_lease = removeJoinShuffleLease,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/durable-join-store.txt", .{tmp.sub_path});
    defer alloc.free(store_path);

    var job_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
    defer job_store.deinit();

    var source = FakeLeaseSource{
        .lease = .{
            .job_id = 77,
            .owner_group_id = 19,
            .expires_at_ms = std.math.maxInt(u64),
        },
    };
    job_store.setContext(source.ctx());

    const state = StatefulShuffleFinalizerState.init(
        source.ctx(),
        &job_store,
        77,
        .{
            .strategy = .shuffle,
            .shuffle_partitions = 8,
        },
        128,
        3,
    );

    try std.testing.expect(state.durable);
    try std.testing.expectEqual(JoinShuffleExecutionMode.durable, state.execution_mode);
    try std.testing.expectEqual(@as(?u64, 77), state.durable_job_id);
    try std.testing.expectEqual(@as(?u64, 19), state.previous_owner_group_id);
}

test "distributed join durable threshold checks require shuffle shared leases and size threshold" {
    const FakeLeaseSource = struct {
        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return null;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn getJoinShuffleLease(_: *anyopaque, _: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            return null;
        }
        fn upsertJoinShuffleLease(_: *anyopaque, _: metadata_table_manager.ShuffleJoinLeaseRecord) !void {}
        fn removeJoinShuffleLease(_: *anyopaque, _: u64) !void {}
        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }
        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }
        fn buildOwnedSearchRequest(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: std.json.Value) !query_api.OwnedQueryRequest {
            return error.UnsupportedOperation;
        }
        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }

        fn ctx(self: *@This()) JoinContext {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .get_join_shuffle_lease = getJoinShuffleLease,
                    .upsert_join_shuffle_lease = upsertJoinShuffleLease,
                    .remove_join_shuffle_lease = removeJoinShuffleLease,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/durable-threshold-store.txt", .{tmp.sub_path});
    defer alloc.free(store_path);

    var no_store = JoinJobStore.init(alloc, .{});
    defer no_store.deinit();
    var ctx_source = FakeLeaseSource{};
    no_store.setContext(ctx_source.ctx());

    try std.testing.expect(!no_store.shouldUseDurableDistributedJoin(.{ .strategy = .broadcast }, 128, 3));
    try std.testing.expect(!no_store.shouldUseDurableDistributedJoin(.{ .strategy = .shuffle, .shuffle_partitions = 8 }, 128, 1));
    try std.testing.expect(!no_store.shouldUseDurableDistributedJoin(.{ .strategy = .shuffle, .shuffle_partitions = 8 }, 128, 3));

    var with_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
    defer with_store.deinit();
    with_store.setContext(ctx_source.ctx());

    try std.testing.expect(!with_store.shouldUseDurableDistributedJoin(.{ .strategy = .shuffle, .shuffle_partitions = 2 }, 32, 3));
    try std.testing.expect(with_store.shouldUseDurableDistributedJoin(.{ .strategy = .shuffle, .shuffle_partitions = 2 }, 64, 3));
    try std.testing.expect(with_store.shouldUseDurableDistributedJoin(.{ .strategy = .shuffle, .shuffle_partitions = 3 }, 8, 3));
}

test "distributed join finalizer start index falls back without usable shared lease" {
    const FakeLeaseSource = struct {
        lease: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,
        fail_lookup: bool = false,
        last_upsert: ?metadata_table_manager.ShuffleJoinLeaseRecord = null,

        fn adminSnapshot(_: *anyopaque) !?metadata_api.AdminSnapshot {
            return null;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn getJoinShuffleLease(ptr: *anyopaque, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail_lookup) return error.Unexpected;
            const lease = self.lease orelse return null;
            if (lease.job_id != job_id) return null;
            return lease;
        }

        fn upsertJoinShuffleLease(ptr: *anyopaque, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_upsert = record;
        }

        fn removeJoinShuffleLease(_: *anyopaque, _: u64) !void {}
        fn executePlainQuery(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) !query_api.QueryResponse {
            return error.UnsupportedOperation;
        }
        fn executeQueryDispatch(_: *anyopaque, _: std.mem.Allocator, _: table_reads.TableReadSource, _: []const u8, _: []const u8, _: ?[]const u8) ![]u8 {
            return error.UnsupportedOperation;
        }
        fn buildOwnedSearchRequest(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: std.json.Value) !query_api.OwnedQueryRequest {
            return error.UnsupportedOperation;
        }
        fn ensureForeignRegistry(_: *anyopaque) !*const foreign_mod.Registry {
            return error.UnsupportedOperation;
        }

        fn ctx(self: *@This()) JoinContext {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .get_join_shuffle_lease = getJoinShuffleLease,
                    .upsert_join_shuffle_lease = upsertJoinShuffleLease,
                    .remove_join_shuffle_lease = removeJoinShuffleLease,
                    .execute_plain_query = executePlainQuery,
                    .execute_query_dispatch = executeQueryDispatch,
                    .build_owned_search_request = buildOwnedSearchRequest,
                    .ensure_foreign_registry = ensureForeignRegistry,
                },
            };
        }
    };

    const alloc = std.testing.allocator;
    const worker_group_ids = [_]u64{ 11, 13, 17 };
    const deterministic = preferredFinalizerStartIndex(41, &worker_group_ids);

    var no_ctx_store = JoinJobStore.init(alloc, .{});
    defer no_ctx_store.deinit();
    try std.testing.expectEqual(deterministic, no_ctx_store.sharedJoinShuffleFinalizerStartIndex(41, &worker_group_ids));

    var missing_owner_source = FakeLeaseSource{
        .lease = .{
            .job_id = 41,
            .owner_group_id = 99,
            .expires_at_ms = std.math.maxInt(u64),
        },
    };
    var missing_owner_store = JoinJobStore.init(alloc, .{});
    defer missing_owner_store.deinit();
    missing_owner_store.setContext(missing_owner_source.ctx());
    try std.testing.expectEqual(deterministic, missing_owner_store.sharedJoinShuffleFinalizerStartIndex(41, &worker_group_ids));
    try std.testing.expectEqual(worker_group_ids[deterministic], missing_owner_source.last_upsert.?.owner_group_id);

    var error_source = FakeLeaseSource{ .fail_lookup = true };
    var error_store = JoinJobStore.init(alloc, .{});
    defer error_store.deinit();
    error_store.setContext(error_source.ctx());
    try std.testing.expectEqual(deterministic, error_store.sharedJoinShuffleFinalizerStartIndex(41, &worker_group_ids));
    try std.testing.expectEqual(worker_group_ids[deterministic], error_source.last_upsert.?.owner_group_id);
}

test "distributed join persisted cached result reloads after restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/persisted-cached-result.txt", .{tmp.sub_path});
    defer alloc.free(store_path);

    {
        var store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
        defer store.deinit();

        try store.recordJoinJobStart(55, 7, 4);

        const hits = try alloc.alloc(std.json.Value, 1);
        errdefer alloc.free(hits);
        hits[0] = try testJoinHitAlloc(alloc, "doc:cached", 2.5, "cached");

        var result = JoinPartitionExecutionResult{
            .hits = hits,
            .stats = .{
                .left_rows_scanned = 3,
                .right_rows_scanned = 5,
                .rows_matched = 1,
            },
            .finalizer_retries = 1,
        };
        defer result.deinit(alloc);

        const encoded = try store.encodeJoinPartitionResponse(alloc, result);
        defer alloc.free(encoded);
        try store.recordJoinJobSucceeded(55, 9, 2, true, encoded);
    }

    {
        var store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
        defer store.deinit();

        var loaded = (try store.loadJoinJobCachedResult(alloc, 55)).?;
        defer loaded.deinit(alloc);

        try std.testing.expectEqual(@as(?u64, 55), loaded.job_id);
        try std.testing.expectEqual(@as(?JoinShuffleJobPhase, .succeeded), loaded.job_phase);
        try std.testing.expectEqual(@as(usize, 4), loaded.total_partitions);
        try std.testing.expectEqual(@as(usize, 4), loaded.completed_partitions);
        try std.testing.expectEqual(@as(usize, 2), loaded.finalizer_retries);
        try std.testing.expectEqual(@as(usize, 1), loaded.hits.len);
        try std.testing.expectEqualStrings("doc:cached", loaded.hits[0].object.get("_id").?.string);
    }
}

test "distributed join persisted resume state reloads after restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/persisted-resume-state.txt", .{tmp.sub_path});
    defer alloc.free(store_path);

    {
        var store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
        defer store.deinit();

        try store.recordJoinJobStart(71, 11, 6);

        const hits = try alloc.alloc(std.json.Value, 1);
        errdefer alloc.free(hits);
        hits[0] = try testJoinHitAlloc(alloc, "doc:partial", 1.0, "partial");

        const matched_ids = try alloc.alloc([]u8, 1);
        errdefer alloc.free(matched_ids);
        matched_ids[0] = try alloc.dupe(u8, "right:partial");

        const worker_attempts = try alloc.alloc(JoinPartitionExecutionResult.WorkerAttempt, 1);
        errdefer alloc.free(worker_attempts);
        worker_attempts[0] = .{
            .partition_index = 0,
            .worker_group_id = 11,
            .succeeded = true,
        };

        var partial = JoinPartitionExecutionResult{
            .hits = hits,
            .stats = .{
                .left_rows_scanned = 4,
                .right_rows_scanned = 8,
                .rows_matched = 2,
            },
            .matched_right_ids = matched_ids,
            .job_id = 71,
            .job_phase = .finalizing,
            .total_partitions = 6,
            .completed_partitions = 2,
            .worker_retries = 3,
            .worker_attempts = worker_attempts,
        };
        defer partial.deinit(alloc);

        try store.recordJoinJobProgress(71, 3, partial);
    }

    {
        var store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = store_path });
        defer store.deinit();

        var loaded_resume = (try store.loadJoinJobResumeState(alloc, 71)).?;
        defer loaded_resume.deinit(alloc);

        try std.testing.expectEqual(@as(usize, 3), loaded_resume.next_partition_index);
        try std.testing.expectEqual(@as(usize, 1), loaded_resume.result.hits.len);
        try std.testing.expectEqualStrings("doc:partial", loaded_resume.result.hits[0].object.get("_id").?.string);
        try std.testing.expectEqual(@as(i64, 4), loaded_resume.result.stats.left_rows_scanned);
        try std.testing.expectEqual(@as(i64, 8), loaded_resume.result.stats.right_rows_scanned);
        try std.testing.expectEqual(@as(i64, 2), loaded_resume.result.stats.rows_matched);
        try std.testing.expectEqual(@as(usize, 2), loaded_resume.result.completed_partitions);
        try std.testing.expectEqual(@as(usize, 3), loaded_resume.result.worker_retries);
        try std.testing.expectEqual(@as(usize, 1), loaded_resume.result.worker_attempts.len);
        try std.testing.expectEqualStrings("right:partial", loaded_resume.result.matched_right_ids[0]);
    }
}

test "distributed join imports remote job state snapshot into local store" {
    const FakeReads = struct {
        snapshot_json: []const u8,
        requested_group_id: ?u64 = null,
        request_count: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
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

        fn joinJobStateGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.request_count += 1;
            self.requested_group_id = group_id;
            return .{
                .json = try alloc.dupe(u8, self.snapshot_json),
            };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const remote_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/remote-join-store.txt", .{tmp.sub_path});
    defer alloc.free(remote_path);
    const local_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/local-join-store.txt", .{tmp.sub_path});
    defer alloc.free(local_path);

    var snapshot_json: []u8 = undefined;
    defer alloc.free(snapshot_json);
    {
        var remote_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = remote_path });
        defer remote_store.deinit();

        try remote_store.recordJoinJobStart(88, 21, 5);

        const hits = try alloc.alloc(std.json.Value, 1);
        errdefer alloc.free(hits);
        hits[0] = try testJoinHitAlloc(alloc, "doc:remote", 2.0, "remote");

        var completed = JoinPartitionExecutionResult{
            .hits = hits,
            .stats = .{
                .left_rows_scanned = 7,
                .right_rows_scanned = 9,
                .rows_matched = 1,
            },
            .job_id = 88,
            .job_phase = .succeeded,
            .total_partitions = 5,
            .completed_partitions = 5,
        };
        defer completed.deinit(alloc);

        const encoded = try remote_store.encodeJoinPartitionResponse(alloc, completed);
        defer alloc.free(encoded);
        try remote_store.recordJoinJobSucceeded(88, 21, 1, false, encoded);

        snapshot_json = (try remote_store.loadJoinJobStateSnapshot(alloc, 88)).?;
    }

    var local_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = local_path });
    defer local_store.deinit();

    var reads = FakeReads{ .snapshot_json = snapshot_json };
    try std.testing.expect(try tryImportRemoteJoinJobState(&local_store, alloc, reads.source(), 21, "customers", 88));
    try std.testing.expectEqual(@as(usize, 1), reads.request_count);
    try std.testing.expectEqual(@as(?u64, 21), reads.requested_group_id);

    var loaded = (try local_store.loadJoinJobCachedResult(alloc, 88)).?;
    defer loaded.deinit(alloc);
    try std.testing.expectEqual(@as(?u64, 88), loaded.job_id);
    try std.testing.expectEqual(@as(?JoinShuffleJobPhase, .succeeded), loaded.job_phase);
    try std.testing.expectEqual(@as(usize, 5), loaded.total_partitions);
    try std.testing.expectEqual(@as(usize, 5), loaded.completed_partitions);
    try std.testing.expectEqualStrings("doc:remote", loaded.hits[0].object.get("_id").?.string);
}

test "distributed join finalizer imports cached result from prior owner" {
    const FakeReads = struct {
        snapshot_json: []const u8,
        request_count: usize = 0,

        fn source(self: *@This()) table_reads.TableReadSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .lookup = lookup,
                    .scan = scan,
                    .query = query,
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

        fn joinJobStateGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?query_api.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.request_count += 1;
            return .{
                .json = try alloc.dupe(u8, self.snapshot_json),
            };
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const alloc = std.testing.allocator;
    const remote_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/remote-finalizer-store.txt", .{tmp.sub_path});
    defer alloc.free(remote_path);
    const local_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/local-finalizer-store.txt", .{tmp.sub_path});
    defer alloc.free(local_path);

    var snapshot_json: []u8 = undefined;
    defer alloc.free(snapshot_json);
    {
        var remote_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = remote_path });
        defer remote_store.deinit();

        try remote_store.recordJoinJobStart(99, 201, 2);

        const hits = try alloc.alloc(std.json.Value, 1);
        errdefer alloc.free(hits);
        hits[0] = try testJoinHitAlloc(alloc, "doc:imported", 4.0, "imported");

        var completed = JoinPartitionExecutionResult{
            .hits = hits,
            .stats = .{
                .left_rows_scanned = 1,
                .right_rows_scanned = 1,
                .rows_matched = 1,
            },
            .job_id = 99,
            .job_phase = .succeeded,
            .total_partitions = 2,
            .completed_partitions = 2,
        };
        defer completed.deinit(alloc);

        const encoded = try remote_store.encodeJoinPartitionResponse(alloc, completed);
        defer alloc.free(encoded);
        try remote_store.recordJoinJobSucceeded(99, 201, 0, false, encoded);

        snapshot_json = (try remote_store.loadJoinJobStateSnapshot(alloc, 99)).?;
    }

    var local_store = try JoinJobStore.initWithStore(alloc, .{ .join_job_store_path = local_path });
    defer local_store.deinit();

    var reads = FakeReads{ .snapshot_json = snapshot_json };
    var finalizer = StatefulShuffleFinalizerState{
        .stable_job_id = 99,
        .durable = true,
        .durable_job_id = 99,
        .previous_owner_group_id = 201,
        .execution_mode = .durable,
    };
    defer finalizer.deinit(alloc);

    var imported = (try finalizer.maybeLoadImportedCachedResult(&local_store, alloc, reads.source(), "customers")).?;
    defer imported.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), reads.request_count);
    try std.testing.expectEqual(@as(?u64, 201), imported.imported_owner_group_id);
    try std.testing.expect(imported.imported_cached_result);
    try std.testing.expectEqualStrings("doc:imported", imported.hits[0].object.get("_id").?.string);
}

fn testContainsString(values: []const []u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

fn testJoinHitAlloc(
    alloc: std.mem.Allocator,
    id: []const u8,
    score: f64,
    source_id: []const u8,
) !std.json.Value {
    var source = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer deinitJsonValue(alloc, &source);
    try source.object.put(alloc, try alloc.dupe(u8, "id"), .{ .string = try alloc.dupe(u8, source_id) });

    var hit = std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer deinitJsonValue(alloc, &hit);
    try hit.object.put(alloc, try alloc.dupe(u8, "_id"), .{ .string = try alloc.dupe(u8, id) });
    try hit.object.put(alloc, try alloc.dupe(u8, "_score"), .{ .float = score });
    try hit.object.put(alloc, try alloc.dupe(u8, "_source"), source);
    source = undefined;
    return hit;
}
