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

//! Import-facing table read callback contract.
//! Implementations stay in table_reads.zig.

const std = @import("std");
const read_gate = @import("../raft/read_gate.zig");
const db_types = @import("../storage/db/types.zig");
const runtime_preflight = @import("../storage/db/runtime_preflight.zig");
const dynamic_field_capability = @import("../storage/db/dynamic_field_capability.zig");
const background_text_stats = @import("../storage/db/background_text_stats.zig");
const distributed_stats_mod = @import("../search/distributed_stats.zig");
const query_api = @import("query_response.zig");
const distributed_graph = @import("distributed_graph.zig");
const runtime_status = @import("runtime_status.zig");
const runtime_callback_abi = @import("../runtime_callback_abi.zig");

pub const LookupResponse = struct {
    json: []u8,
    version: u64,

    pub fn deinit(self: *LookupResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.json);
        self.* = undefined;
    }
};

pub const ScanResponse = struct {
    ndjson: []u8,

    pub fn deinit(self: *ScanResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.ndjson);
        self.* = undefined;
    }
};

pub const TextStatsResponse = struct {
    fields: []const distributed_stats_mod.TextFieldStats,

    pub fn deinit(self: *TextStatsResponse, alloc: std.mem.Allocator) void {
        distributed_stats_mod.deinitTextFieldStats(alloc, self.fields);
        self.* = undefined;
    }
};

pub const BackgroundTextStatsResponse = struct {
    background_fields: []const background_text_stats.DistributedBackgroundTextStats,

    pub fn deinit(self: *BackgroundTextStatsResponse, alloc: std.mem.Allocator) void {
        background_text_stats.deinitAll(alloc, self.background_fields);
        self.* = undefined;
    }
};

pub const LsmStorageStats = runtime_status.LsmStorageStats;
pub const ObservedDynamicFieldCapabilitySet = dynamic_field_capability.ObservedDynamicFieldCapabilitySet;

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
pub const TableReadSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: BoundaryAbi.Dispatch = BoundaryAbi.local_dispatch,

    pub const VTable = struct {
        lookup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            key: []const u8,
            opts: db_types.LookupOptions,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?LookupResponse,
        scan: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_types.ScanOptions,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?ScanResponse,
        query: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_types.SearchRequest,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?query_api.QueryResponse,
        preflight_query: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_types.SearchRequest,
            consistency: read_gate.ReadConsistency,
            max_work: u32,
        ) anyerror!?runtime_preflight.RuntimePreflightSummary = null,
        preflight_query_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_types.SearchRequest,
            consistency: read_gate.ReadConsistency,
            max_work: u32,
        ) anyerror!?runtime_preflight.RuntimePreflightSummary = null,
        lookup_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            key: []const u8,
            opts: db_types.LookupOptions,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?LookupResponse = null,
        scan_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            from_key: []const u8,
            to_key: []const u8,
            opts: db_types.ScanOptions,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?ScanResponse = null,
        query_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_types.SearchRequest,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?query_api.QueryResponse = null,
        search_result_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: db_types.SearchRequest,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?db_types.SearchResult = null,
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
        join_partition_group_local_with_timeout: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
            timeout_ms: ?u32,
        ) anyerror!?query_api.QueryResponse = null,
        join_rows_group_local_with_timeout: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
            timeout_ms: ?u32,
        ) anyerror!?query_api.QueryResponse = null,
        join_unmatched_group_local_with_timeout: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
            timeout_ms: ?u32,
        ) anyerror!?query_api.QueryResponse = null,
        join_finalize_group_local_with_timeout: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            body: []const u8,
            timeout_ms: ?u32,
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
            consistency: read_gate.ReadConsistency,
        ) anyerror!?distributed_graph.GraphExpandResponse = null,
        graph_hydrate_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphHydrateRequest,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?distributed_graph.GraphHydrateResponse = null,
        graph_edges_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: distributed_graph.GraphEdgesRequest,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?distributed_graph.GraphEdgesResponse = null,
        local_runtime_statuses: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?runtime_status.LocalTableRuntimeStatuses = null,
        lsm_storage_stats: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?LsmStorageStats = null,
        observed_dynamic_field_capability_sets: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) anyerror!?[]ObservedDynamicFieldCapabilitySet = null,
        document_artifact_manifest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?db_types.DocumentArtifactManifest = null,
        document_artifact_manifests: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?db_types.DocumentArtifactManifestList = null,
        document_artifact_manifest_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?db_types.DocumentArtifactManifest = null,
        document_artifact_manifests_group_local: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            doc_key: []const u8,
            consistency: read_gate.ReadConsistency,
        ) anyerror!?db_types.DocumentArtifactManifestList = null,
    };
    const BoundaryAbi = runtime_callback_abi.Boundary(VTable);

    pub fn lookup(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?LookupResponse {
        return try BoundaryAbi.call("lookup", self.boundary_dispatch, self.vtable.lookup, .{ self.ptr, alloc, table_name, key, opts, consistency });
    }

    pub fn scan(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?ScanResponse {
        return try BoundaryAbi.call("scan", self.boundary_dispatch, self.vtable.scan, .{ self.ptr, alloc, table_name, from_key, to_key, opts, consistency });
    }

    pub fn documentArtifactManifest(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifest {
        const fn_ptr = self.vtable.document_artifact_manifest orelse return null;
        return try BoundaryAbi.call("document_artifact_manifest", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, doc_key, artifact_name, consistency });
    }

    pub fn documentArtifactManifestGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifest {
        const fn_ptr = self.vtable.document_artifact_manifest_group_local orelse return null;
        return try BoundaryAbi.call("document_artifact_manifest_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, doc_key, artifact_name, consistency });
    }

    pub fn documentArtifactManifests(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifestList {
        const fn_ptr = self.vtable.document_artifact_manifests orelse return null;
        return try BoundaryAbi.call("document_artifact_manifests", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, doc_key, consistency });
    }

    pub fn documentArtifactManifestsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifestList {
        const fn_ptr = self.vtable.document_artifact_manifests_group_local orelse return null;
        return try BoundaryAbi.call("document_artifact_manifests_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, doc_key, consistency });
    }

    pub fn query(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_api.QueryResponse {
        return try BoundaryAbi.call("query", self.boundary_dispatch, self.vtable.query, .{ self.ptr, alloc, table_name, req, consistency });
    }

    pub fn preflightQuery(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const fn_ptr = self.vtable.preflight_query orelse return null;
        return try BoundaryAbi.call("preflight_query", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name, req, consistency, max_work });
    }

    pub fn preflightQueryGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const fn_ptr = self.vtable.preflight_query_group_local orelse return null;
        return try BoundaryAbi.call("preflight_query_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req, consistency, max_work });
    }

    pub fn lookupGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?LookupResponse {
        const fn_ptr = self.vtable.lookup_group_local orelse return null;
        return try BoundaryAbi.call("lookup_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, key, opts, consistency });
    }

    pub fn scanGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?ScanResponse {
        const fn_ptr = self.vtable.scan_group_local orelse return null;
        return try BoundaryAbi.call("scan_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency });
    }

    pub fn queryGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.query_group_local orelse return null;
        return try BoundaryAbi.call("query_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req, consistency });
    }

    pub fn searchResultGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.SearchResult {
        const fn_ptr = self.vtable.search_result_group_local orelse return null;
        return try BoundaryAbi.call("search_result_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req, consistency });
    }

    pub fn textStatsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.text_stats_group_local orelse return null;
        return try BoundaryAbi.call("text_stats_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn algebraicPartialsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.algebraic_partials_group_local orelse return null;
        return try BoundaryAbi.call("algebraic_partials_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn joinPartitionGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_partition_group_local orelse return null;
        return try BoundaryAbi.call("join_partition_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn joinRowsGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_rows_group_local orelse return null;
        return try BoundaryAbi.call("join_rows_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn joinUnmatchedGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_unmatched_group_local orelse return null;
        return try BoundaryAbi.call("join_unmatched_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn joinFinalizeGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_finalize_group_local orelse return null;
        return try BoundaryAbi.call("join_finalize_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn joinPartitionGroupLocalWithTimeout(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !?query_api.QueryResponse {
        if (self.vtable.join_partition_group_local_with_timeout) |fn_ptr| {
            return try BoundaryAbi.call("join_partition_group_local_with_timeout", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body, timeout_ms });
        }
        if (timeout_ms != null) return error.UnsupportedDeadline;
        return try self.joinPartitionGroupLocal(alloc, group_id, table_name, body);
    }

    pub fn joinRowsGroupLocalWithTimeout(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !?query_api.QueryResponse {
        if (self.vtable.join_rows_group_local_with_timeout) |fn_ptr| {
            return try BoundaryAbi.call("join_rows_group_local_with_timeout", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body, timeout_ms });
        }
        if (timeout_ms != null) return error.UnsupportedDeadline;
        return try self.joinRowsGroupLocal(alloc, group_id, table_name, body);
    }

    pub fn joinUnmatchedGroupLocalWithTimeout(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !?query_api.QueryResponse {
        if (self.vtable.join_unmatched_group_local_with_timeout) |fn_ptr| {
            return try BoundaryAbi.call("join_unmatched_group_local_with_timeout", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body, timeout_ms });
        }
        if (timeout_ms != null) return error.UnsupportedDeadline;
        return try self.joinUnmatchedGroupLocal(alloc, group_id, table_name, body);
    }

    pub fn joinFinalizeGroupLocalWithTimeout(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        timeout_ms: ?u32,
    ) !?query_api.QueryResponse {
        if (self.vtable.join_finalize_group_local_with_timeout) |fn_ptr| {
            return try BoundaryAbi.call("join_finalize_group_local_with_timeout", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body, timeout_ms });
        }
        if (timeout_ms != null) return error.UnsupportedDeadline;
        return try self.joinFinalizeGroupLocal(alloc, group_id, table_name, body);
    }

    pub fn joinJobStateGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_api.QueryResponse {
        const fn_ptr = self.vtable.join_job_state_group_local orelse return null;
        return try BoundaryAbi.call("join_job_state_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, body });
    }

    pub fn graphExpandGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const fn_ptr = self.vtable.graph_expand_group_local orelse return null;
        return try BoundaryAbi.call("graph_expand_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req, consistency });
    }

    pub fn graphHydrateGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const fn_ptr = self.vtable.graph_hydrate_group_local orelse return null;
        return try BoundaryAbi.call("graph_hydrate_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req, consistency });
    }

    pub fn graphEdgesGroupLocal(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const fn_ptr = self.vtable.graph_edges_group_local orelse return null;
        return try BoundaryAbi.call("graph_edges_group_local", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, group_id, table_name, req, consistency });
    }

    pub fn localRuntimeStatuses(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const fn_ptr = self.vtable.local_runtime_statuses orelse return null;
        return try BoundaryAbi.call("local_runtime_statuses", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name });
    }

    pub fn lsmStorageStats(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?LsmStorageStats {
        const fn_ptr = self.vtable.lsm_storage_stats orelse return null;
        return try BoundaryAbi.call("lsm_storage_stats", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name });
    }

    pub fn observedDynamicFieldCapabilitySets(
        self: TableReadSource,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?[]ObservedDynamicFieldCapabilitySet {
        const fn_ptr = self.vtable.observed_dynamic_field_capability_sets orelse return null;
        return try BoundaryAbi.call("observed_dynamic_field_capability_sets", self.boundary_dispatch, fn_ptr, .{ self.ptr, alloc, table_name });
    }
};
