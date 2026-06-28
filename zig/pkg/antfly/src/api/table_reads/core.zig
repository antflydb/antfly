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

const db_mod = @import("../../storage/db/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const catalog_resources = @import("../catalog_resources.zig");
const query_api = @import("../query.zig");
const document_sql_runtime = @import("document_sql_runtime.zig");
const raft_mod = @import("../../raft/mod.zig");
const ha_read_gate_mod = @import("../../storage/ha/read_gate.zig");
const ha_standby_mod = @import("../../storage/ha/standby.zig");
const serverless_query = @import("../../serverless/query/mod.zig");
const distributed_graph = @import("../distributed_graph.zig");
const runtime_status = @import("../runtime_status.zig");
const distributed_stats_mod = @import("../../search/distributed_stats.zig");
const table_router = @import("../table_router.zig");

pub const LookupResponse = document_sql_runtime.LookupResponse;
pub const ScanResponse = document_sql_runtime.ScanResponse;

pub fn appendScanLine(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    projected_json: ?[]const u8,
) !void {
    const escaped_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
    defer alloc.free(escaped_key);

    try out.appendSlice(alloc, "{\"key\":");
    try out.appendSlice(alloc, escaped_key);
    if (projected_json) |json| {
        if (json.len < 2 or json[0] != '{' or json[json.len - 1] != '}') return error.InvalidProjectedDocumentJson;
        if (json.len > 2) {
            try out.append(alloc, ',');
            try out.appendSlice(alloc, json[1..]);
        } else {
            try out.append(alloc, '}');
        }
    } else {
        try out.append(alloc, '}');
    }
    try out.append(alloc, '\n');
}

pub const TextStatsResponse = struct {
    fields: []const distributed_stats_mod.TextFieldStats,

    pub fn deinit(self: *TextStatsResponse, alloc: std.mem.Allocator) void {
        distributed_stats_mod.deinitTextFieldStats(alloc, self.fields);
        self.* = undefined;
    }
};

pub const BackgroundTextStatsResponse = struct {
    background_fields: []const db_mod.aggregations.DistributedBackgroundTextStats,

    pub fn deinit(self: *BackgroundTextStatsResponse, alloc: std.mem.Allocator) void {
        db_mod.aggregations.deinitDistributedBackgroundTextStats(alloc, self.background_fields);
        self.* = undefined;
    }
};

pub const LsmStorageStats = runtime_status.LsmStorageStats;

pub const ParsedTextStatsHttpResponse = union(enum) {
    fields: TextStatsResponse,
    background_fields: BackgroundTextStatsResponse,

    pub fn deinit(self: *ParsedTextStatsHttpResponse, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .fields => |*value| value.deinit(alloc),
            .background_fields => |*value| value.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const HAReadGate = struct {
    standby: *const ha_standby_mod.Standby,

    pub fn check(self: HAReadGate, consistency: raft_mod.ReadConsistency) !void {
        const decision = try ha_read_gate_mod.evaluateStandby(self.standby, .{
            .consistency = switch (consistency) {
                .stale => .stale_ok,
                .leader_lease, .read_index => .primary,
            },
        });
        switch (decision.action) {
            .serve_standby => {},
            .wait_for_apply => return error.HAReadWaitForApply,
            .wait_for_metadata => return error.HAReadWaitForMetadata,
            .route_to_primary => return error.HAReadRequiresPrimary,
        }
    }
};

pub fn routePolicyForConsistency(consistency: raft_mod.ReadConsistency) table_router.RoutePolicy {
    return switch (consistency) {
        .stale => .any_active,
        .leader_lease, .read_index => .prefer_leader,
    };
}

pub const DocIdentityInternalWorkerBoundary = enum {
    query,
    vector_worker,
    preflight,
    graph_expand,
    graph_hydrate,
    graph_edges,
    search_request_text_stats,
    explicit_text_stats,
    background_text_stats,
    aggregation_context,
    aggregation_full_result_rerun,
    algebraic_partials,
    distributed_join_right_fanout,
    distributed_join_worker,
    distributed_join_unmatched_followup,
    distributed_join_finalizer,
    shuffle_worker,
    shuffle_finalizer,
    graph_result_ref,
};

pub const DocIdentityInternalWorkerPolicy = enum {
    carries_shard_doc_set,
    validates_generation_projection,
    fail_closed_before_fanout,
};

pub fn docIdentityInternalWorkerPolicy(boundary: DocIdentityInternalWorkerBoundary) DocIdentityInternalWorkerPolicy {
    return switch (boundary) {
        .query,
        .vector_worker,
        .preflight,
        .graph_expand,
        .graph_hydrate,
        .search_request_text_stats,
        .explicit_text_stats,
        .background_text_stats,
        => .carries_shard_doc_set,

        .graph_edges,
        .aggregation_context,
        .aggregation_full_result_rerun,
        .algebraic_partials,
        .distributed_join_right_fanout,
        .distributed_join_worker,
        .distributed_join_unmatched_followup,
        .distributed_join_finalizer,
        .shuffle_worker,
        .shuffle_finalizer,
        .graph_result_ref,
        => .validates_generation_projection,
    };
}

test "internal worker doc identity exchange audit covers every boundary" {
    var carries_shard_doc_set: usize = 0;
    var validates_generation_projection: usize = 0;
    var fail_closed_before_fanout: usize = 0;

    inline for (std.meta.fields(DocIdentityInternalWorkerBoundary)) |field| {
        const boundary: DocIdentityInternalWorkerBoundary = @field(DocIdentityInternalWorkerBoundary, field.name);
        switch (docIdentityInternalWorkerPolicy(boundary)) {
            .carries_shard_doc_set => carries_shard_doc_set += 1,
            .validates_generation_projection => validates_generation_projection += 1,
            .fail_closed_before_fanout => fail_closed_before_fanout += 1,
        }
    }

    try std.testing.expectEqual(@as(usize, 8), carries_shard_doc_set);
    try std.testing.expectEqual(@as(usize, 11), validates_generation_projection);
    try std.testing.expectEqual(@as(usize, 0), fail_closed_before_fanout);
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.query));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.vector_worker));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.search_request_text_stats));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.distributed_join_worker));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.distributed_join_unmatched_followup));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.shuffle_worker));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.algebraic_partials));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.aggregation_full_result_rerun));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.validates_generation_projection, docIdentityInternalWorkerPolicy(.graph_result_ref));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.explicit_text_stats));
    try std.testing.expectEqual(DocIdentityInternalWorkerPolicy.carries_shard_doc_set, docIdentityInternalWorkerPolicy(.background_text_stats));
}

pub const ReadPreparation = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Kind = enum {
        general,
        dense_query,
    };

    pub const VTable = struct {
        prepare_for_read: *const fn (ptr: *anyopaque, table_name: []const u8, kind: Kind) void,
    };

    pub fn prepareForRead(self: ReadPreparation, table_name: []const u8, kind: Kind) void {
        self.vtable.prepare_for_read(self.ptr, table_name, kind);
    }
};

/// Shared LSM/HBC cache namespace used when callers want the storage backend's
/// current root instead of a reconciled group-visible root snapshot.
pub const backend_current_root_generation: u64 = 0;

pub const GroupVisibleRootGenerationSource = struct {
    ptr: *anyopaque,
    visible_root_generation_for_group: *const fn (ptr: *anyopaque, group_id: u64) u64,

    /// Shared LSM/HBC cache namespace for the currently visible replica root.
    /// This is advanced when local root/catalog visibility is reconciled; it is
    /// not the storage engine's physical per-write generation.
    pub fn visibleRootGenerationForGroup(self: GroupVisibleRootGenerationSource, group_id: u64) u64 {
        return self.visible_root_generation_for_group(self.ptr, group_id);
    }
};

pub const PrimaryLookupDbLease = struct {
    ptr: *anyopaque,
    db: *db_mod.DB,
    release_fn: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) void,

    pub fn release(self: *PrimaryLookupDbLease, alloc: std.mem.Allocator) void {
        self.release_fn(self.ptr, alloc);
        self.* = undefined;
    }
};

pub const PrimaryLookupDbSource = struct {
    ptr: *anyopaque,
    lease_group: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
    ) anyerror!?PrimaryLookupDbLease,

    pub fn leaseGroup(
        self: PrimaryLookupDbSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
    ) !?PrimaryLookupDbLease {
        return try self.lease_group(self.ptr, alloc, table_name, group_id, lsm_root_generation);
    }
};

pub const RelationalRowsSourceGroupRequest = struct {
    schema_json: []const u8,
    topology_epoch: u64,
    req: db_mod.types.RelationalRowsQueryRequest,
    doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
};

pub const TableReadSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        lookup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?LookupResponse,
        lookup_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?LookupResponse = null,
        scan: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?ScanResponse,
        scan_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?ScanResponse = null,
        query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse,
        query_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse = null,
        document_algebraic_aggregate: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: document_sql_runtime.AlgebraicAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?document_sql_runtime.AlgebraicAggregateResponse = null,
        document_algebraic_aggregate_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            req: document_sql_runtime.AlgebraicAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?document_sql_runtime.AlgebraicAggregateResponse = null,
        document_algebraic_aggregate_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: document_sql_runtime.AlgebraicAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?document_sql_runtime.AlgebraicAggregateResponse = null,
        preflight_query: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
            max_work: u32,
        ) anyerror!?db_mod.RuntimePreflightSummary = null,
        preflight_query_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
            max_work: u32,
        ) anyerror!?db_mod.RuntimePreflightSummary = null,
        lookup_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            key: []const u8,
            opts: db_mod.types.LookupOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?LookupResponse = null,
        relational_unique_owner_lookup: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?[]u8 = null,
        relational_temporal_unique_owner_lookup: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
            encoded_point: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?[]u8 = null,
        relational_temporal_unique_overlap_owner_lookup: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
            encoded_start: []const u8,
            encoded_end: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?[]u8 = null,
        relational_temporal_unique_owner_lookup_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
            encoded_point: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?[]u8 = null,
        relational_temporal_unique_overlap_owner_lookup_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            constraint_name: []const u8,
            encoded_value: []const u8,
            encoded_start: []const u8,
            encoded_end: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?[]u8 = null,
        rows_query_plan: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult = null,
        rows_query_plan_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult = null,
        rows_query_plan_system_time_as_of_sequence: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            commit_sequence: u64,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult = null,
        rows_query_plan_catalog_system_time_as_of_sequence: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            commit_sequence: u64,
            plan: db_mod.types.RelationalRowsQueryPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult = null,
        rows_set_operation_plan: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult = null,
        rows_set_operation_plan_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsSetOperationPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult = null,
        rows_aggregate_plan: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsAggregatePlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsAggregateResult = null,
        lake_rows_scan: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            request: serverless_query.LakeRowsScanRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?serverless_query.LakeRowsScanResult = null,
        lake_rows_expression_aggregates: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            request: serverless_query.LakeRowsExpressionAggregateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?serverless_query.LakeRowsExpressionAggregateResult = null,
        rows_window_plan: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsWindowPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsWindowResult = null,
        rows_join_plan: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsJoinPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsJoinResult = null,
        rows_lateral_plan: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            runtime_schema: storage_schema.TableSchema,
            plan: db_mod.types.RelationalRowsLateralPlan,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsJoinResult = null,
        relational_rows_source_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            schema_json: []const u8,
            topology_epoch: u64,
            req: db_mod.types.RelationalRowsQueryRequest,
            doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.RelationalRowsQueryResult = null,
        scan_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_mod.types.ScanOptions,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?ScanResponse = null,
        query_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?query_api.QueryResponse = null,
        search_result_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_mod.types.SearchRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.SearchResult = null,
        text_stats_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        algebraic_partials_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_partition_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_rows_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_unmatched_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_finalize_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        join_job_state_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
        ) anyerror!?query_api.QueryResponse = null,
        graph_expand_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphExpandRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?distributed_graph.GraphExpandResponse = null,
        graph_hydrate_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphHydrateRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?distributed_graph.GraphHydrateResponse = null,
        graph_edges_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphEdgesRequest,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?distributed_graph.GraphEdgesResponse = null,
        local_runtime_statuses: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
        local_runtime_statuses_catalog: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            target: catalog_resources.TableTarget,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
        lsm_storage_stats: ?*const fn (
            ptr: *anyopaque,
            table_name: []const u8,
        ) anyerror!?LsmStorageStats = null,
        document_artifact_manifest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.DocumentArtifactManifest = null,
        document_artifact_manifests: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.DocumentArtifactManifestList = null,
        document_artifact_manifest_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.DocumentArtifactManifest = null,
        document_artifact_manifests_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            consistency: raft_mod.ReadConsistency,
        ) anyerror!?db_mod.types.DocumentArtifactManifestList = null,
    };

    pub fn lookup(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        return try self.vtable.lookup(self.ptr, alloc, table_name, key, opts, consistency);
    }

    pub fn lookupCatalog(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        if (self.vtable.lookup_catalog) |fn_ptr| return try fn_ptr(self.ptr, alloc, target, key, opts, consistency);
        return error.UnsupportedOperation;
    }

    pub fn scan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        return try self.vtable.scan(self.ptr, alloc, table_name, from_key, to_key, opts, consistency);
    }

    pub fn scanCatalog(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const fn_ptr = self.vtable.scan_catalog orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, target, from_key, to_key, opts, consistency);
    }

    pub fn documentArtifactManifest(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifest {
        const fn_ptr = self.vtable.document_artifact_manifest orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, artifact_name, consistency);
    }

    pub fn documentArtifactManifestGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifest {
        const fn_ptr = self.vtable.document_artifact_manifest_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, doc_key, artifact_name, consistency);
    }

    pub fn documentArtifactManifests(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifestList {
        const fn_ptr = self.vtable.document_artifact_manifests orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, consistency);
    }

    pub fn query(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        return try self.vtable.query(self.ptr, alloc, table_name, req, consistency);
    }

    pub fn queryCatalog(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        if (self.vtable.query_catalog) |fn_ptr| return try fn_ptr(self.ptr, alloc, target, req, consistency);
        return error.UnsupportedOperation;
    }

    pub fn documentAlgebraicAggregate(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const fn_ptr = self.vtable.document_algebraic_aggregate orelse return error.DocumentSqlIndexUnavailable;
        return try fn_ptr(self.ptr, alloc, table_name, req, consistency);
    }

    pub fn documentAlgebraicAggregateCatalog(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const fn_ptr = self.vtable.document_algebraic_aggregate_catalog orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, target, req, consistency);
    }

    pub fn documentAlgebraicAggregateGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: document_sql_runtime.AlgebraicAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?document_sql_runtime.AlgebraicAggregateResponse {
        const fn_ptr = self.vtable.document_algebraic_aggregate_group_local orelse return error.DocumentSqlIndexUnavailable;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn preflightQuery(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const fn_ptr = self.vtable.preflight_query orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name, req, consistency, max_work);
    }

    pub fn rowsQueryPlan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const fn_ptr = self.vtable.rows_query_plan orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    pub fn rowsQueryPlanCatalog(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const fn_ptr = self.vtable.rows_query_plan_catalog orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, target, runtime_schema, plan, consistency);
    }

    pub fn rowsQueryPlanSystemTimeAsOfSequence(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const fn_ptr = self.vtable.rows_query_plan_system_time_as_of_sequence orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, commit_sequence, plan, consistency);
    }

    pub fn rowsQueryPlanCatalogSystemTimeAsOfSequence(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        commit_sequence: u64,
        plan: db_mod.types.RelationalRowsQueryPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const fn_ptr = self.vtable.rows_query_plan_catalog_system_time_as_of_sequence orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, target, runtime_schema, commit_sequence, plan, consistency);
    }

    pub fn rowsSetOperationPlan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsSetOperationPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const fn_ptr = self.vtable.rows_set_operation_plan orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    pub fn rowsSetOperationPlanCatalog(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsSetOperationPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const fn_ptr = self.vtable.rows_set_operation_plan_catalog orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, target, runtime_schema, plan, consistency);
    }

    pub fn rowsAggregatePlan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsAggregatePlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsAggregateResult {
        const fn_ptr = self.vtable.rows_aggregate_plan orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    pub fn lakeRowsScan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsScanRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?serverless_query.LakeRowsScanResult {
        const fn_ptr = self.vtable.lake_rows_scan orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, request, consistency);
    }

    pub fn lakeRowsExpressionAggregates(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        request: serverless_query.LakeRowsExpressionAggregateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?serverless_query.LakeRowsExpressionAggregateResult {
        const fn_ptr = self.vtable.lake_rows_expression_aggregates orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, request, consistency);
    }

    pub fn rowsWindowPlan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsWindowPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsWindowResult {
        const fn_ptr = self.vtable.rows_window_plan orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    pub fn rowsJoinPlan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsJoinPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const fn_ptr = self.vtable.rows_join_plan orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    pub fn rowsLateralPlan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        runtime_schema: storage_schema.TableSchema,
        plan: db_mod.types.RelationalRowsLateralPlan,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsJoinResult {
        const fn_ptr = self.vtable.rows_lateral_plan orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, runtime_schema, plan, consistency);
    }

    pub fn relationalRowsSourceGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        schema_json: []const u8,
        topology_epoch: u64,
        req: db_mod.types.RelationalRowsQueryRequest,
        doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.RelationalRowsQueryResult {
        const fn_ptr = self.vtable.relational_rows_source_group_local orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, schema_json, topology_epoch, req, doc_key_range, consistency);
    }

    pub fn preflightQueryGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
        max_work: u32,
    ) !?db_mod.RuntimePreflightSummary {
        const fn_ptr = self.vtable.preflight_query_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency, max_work);
    }

    pub fn lookupGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_mod.types.LookupOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?LookupResponse {
        const fn_ptr = self.vtable.lookup_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, key, opts, consistency);
    }

    pub fn documentArtifactManifestsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.DocumentArtifactManifestList {
        const fn_ptr = self.vtable.document_artifact_manifests_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, doc_key, consistency);
    }

    pub fn relationalUniqueOwnerLookup(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const fn_ptr = self.vtable.relational_unique_owner_lookup orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, constraint_name, encoded_value, consistency);
    }

    pub fn relationalTemporalUniqueOwnerLookup(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const fn_ptr = self.vtable.relational_temporal_unique_owner_lookup orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, constraint_name, encoded_value, encoded_point, consistency);
    }

    pub fn relationalTemporalUniqueOverlapOwnerLookup(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const fn_ptr = self.vtable.relational_temporal_unique_overlap_owner_lookup orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, table_name, constraint_name, encoded_value, encoded_start, encoded_end, consistency);
    }

    pub fn relationalTemporalUniqueOwnerLookupGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_point: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const fn_ptr = self.vtable.relational_temporal_unique_owner_lookup_group_local orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, constraint_name, encoded_value, encoded_point, consistency);
    }

    pub fn relationalTemporalUniqueOverlapOwnerLookupGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        constraint_name: []const u8,
        encoded_value: []const u8,
        encoded_start: []const u8,
        encoded_end: []const u8,
        consistency: raft_mod.ReadConsistency,
    ) !?[]u8 {
        const fn_ptr = self.vtable.relational_temporal_unique_overlap_owner_lookup_group_local orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, constraint_name, encoded_value, encoded_start, encoded_end, consistency);
    }

    pub fn scanGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_mod.types.ScanOptions,
        consistency: raft_mod.ReadConsistency,
    ) !?ScanResponse {
        const fn_ptr = self.vtable.scan_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency);
    }

    pub fn queryGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.query_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn searchResultGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.SearchRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?db_mod.types.SearchResult {
        const fn_ptr = self.vtable.search_result_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn textStatsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.text_stats_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn algebraicPartialsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.algebraic_partials_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinPartitionGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_partition_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinRowsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_rows_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinUnmatchedGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_unmatched_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinFinalizeGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_finalize_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn joinJobStateGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_job_state_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, body);
    }

    pub fn graphExpandGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const fn_ptr = self.vtable.graph_expand_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn graphHydrateGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const fn_ptr = self.vtable.graph_hydrate_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn graphEdgesGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: raft_mod.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const fn_ptr = self.vtable.graph_edges_group_local orelse return null;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req, consistency);
    }

    pub fn localRuntimeStatuses(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const fn_ptr = self.vtable.local_runtime_statuses orelse return null;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn localRuntimeStatusesCatalog(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        target: catalog_resources.TableTarget,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        if (self.vtable.local_runtime_statuses_catalog) |fn_ptr| return try fn_ptr(self.ptr, alloc, target);
        return error.UnsupportedOperation;
    }

    pub fn lsmStorageStats(
        self: TableReadSource,
        table_name: []const u8,
    ) !?LsmStorageStats {
        const fn_ptr = self.vtable.lsm_storage_stats orelse return null;
        return try fn_ptr(self.ptr, table_name);
    }
};
