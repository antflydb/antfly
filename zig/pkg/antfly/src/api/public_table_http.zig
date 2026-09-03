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
const builtin = @import("builtin");
const metadata_openapi = @import("antfly_metadata_openapi");
const backups_api = @import("backups.zig");
const batch_api = @import("batch.zig");
const db_mod = @import("../storage/db/mod.zig");
const graph_pattern_mod = @import("../graph/pattern.zig");
const graph_query_mod = @import("../graph/query.zig");
const graph_distinct_budget_diagnostic = @import("../graph/distinct_budget_diagnostic.zig");
const graph_work_budget_diagnostic = @import("../graph/work_budget_diagnostic.zig");
const graph_path_weight_diagnostic = @import("../graph/path_weight_diagnostic.zig");
const graph_query_diagnostic = @import("graph_query_diagnostic.zig");
const graph_request_diagnostics = @import("graph_request_diagnostics.zig");
const common_secrets = @import("../common/secrets.zig");
const common_config = @import("../common/config.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const query_contract = @import("query_contract.zig");
const operation = @import("operation.zig");

threadlocal var last_batch_failure_name: ?[]const u8 = null;

pub fn resetLastBatchFailureName() void {
    last_batch_failure_name = null;
}

pub fn setLastBatchFailureName(err: anyerror) void {
    last_batch_failure_name = @errorName(err);
}

fn takeLastBatchFailureName() ?[]const u8 {
    const name = last_batch_failure_name;
    last_batch_failure_name = null;
    return name;
}

pub const DocumentArtifactManifestDetail = enum {
    summary,
    raw,
};

pub const DocumentArtifactManifestOptions = struct {
    detail: DocumentArtifactManifestDetail = .raw,
};

pub fn parseDocumentArtifactManifestOptions(alloc: std.mem.Allocator, query: []const u8) !DocumentArtifactManifestOptions {
    var opts = DocumentArtifactManifestOptions{};
    if (query.len == 0) return opts;
    var it = std.mem.splitScalar(u8, query, '&');
    while (it.next()) |part| {
        if (!std.mem.startsWith(u8, part, "detail=")) continue;
        const value = try http_route_helpers.decodePercentEncodedPathComponentAlloc(alloc, part["detail=".len..]);
        defer alloc.free(value);
        if (std.mem.eql(u8, value, "summary")) {
            opts.detail = .summary;
        } else if (std.mem.eql(u8, value, "raw")) {
            opts.detail = .raw;
        } else {
            return error.InvalidDetail;
        }
    }
    return opts;
}

pub const TableApi = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    /// Required transport-neutral context. Detached and test callers must opt
    /// into `.none` explicitly via an empty RequestContext.
    request: operation.RequestContext,

    fn ensureActive(self: TableApi) error{ Canceled, DeadlineExceeded }!void {
        self.request.ensureActive() catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.DeadlineExceeded => return error.DeadlineExceeded,
            else => unreachable,
        };
    }

    pub const ExecuteBatchError = error{
        InvalidBatchRequest,
        UnsupportedSyncLevel,
        NotFound,
        Conflict,
        MethodNotAllowed,
        Backpressured,
        DenseRepairBackpressure,
        Unavailable,
        WriteUnavailable,
        HAWriteDurabilityPending,
        OutcomeUnknown,
        CommittedPending,
        CommittedRepairRequired,
        WriteOutcomeUnknown,
        DocIdentityUnavailable,
        HAReadOnlyStandby,
        HAPromotedStandbyRequiresPrimaryOpen,
        HAFencedPrimary,
        Canceled,
        DeadlineExceeded,
        InternalFailure,
    };

    pub const ExecuteQueryError = error{
        InvalidQueryRequest,
        InvalidFilterQueryRequest,
        InvalidExclusionQueryRequest,
        UnsupportedFilterQueryRequest,
        UnsupportedExclusionQueryRequest,
        UnsupportedQueryRequest,
        UnsupportedHierarchyGrouping,
        NotFound,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        StorageReadTemporarilyUnavailable,
        IndexRebuilding,
        ModelNotFound,
        UnsupportedExactSort,
        QueryCandidateBudgetExceeded,
        GraphWorkBudgetExceeded,
        GraphMinWeightDomainViolation,
        GraphMaxWeightDomainViolation,
        GraphPathWeightOverflow,
        GraphDistinctBudgetExceeded,
        GraphAnchorFilterRequiresIndex,
        GraphMatchOperationLimitExceeded,
        GraphQueryModeUnsupported,
        GraphExternalAliasDocumentFilterUnsupported,
        GraphExternalAliasSourceUnsupported,
        GraphReverseVariablePathUnsupported,
        HierarchyCursorStale,
        TopologyChanged,
        QueryEmbeddingInputTooLarge,
        QueryEmbeddingOverloaded,
        EmbedRateLimited,
        EmbedTransientFailure,
        EmbedUpstreamFailure,
        Canceled,
        DeadlineExceeded,
        InvalidManifest,
        InvalidTableFile,
        TableBlockChecksumMismatch,
        CorruptInput,
        UnsupportedVersion,
        Corrupted,
        IncompletePublishedSnapshot,
        InternalFailure,
    };

    pub const ExecuteQueryViewError = error{
        NotFound,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        StorageReadTemporarilyUnavailable,
        ModelNotFound,
        Canceled,
        DeadlineExceeded,
        InternalFailure,
    };

    pub const ExecuteBackupError = error{
        Canceled,
        DeadlineExceeded,
        MetadataCapabilityUnavailable,
        NotLeader,
        NotFound,
        CatalogChanged,
        BackupAlreadyExists,
        BackupOutcomeAmbiguous,
        MethodNotAllowed,
        BackupManifestTooLarge,
        UnsupportedBackupFormat,
        UnsupportedBackupMigrationState,
        UnsupportedMultiRangeTable,
        InternalFailure,
    };

    pub const ExecuteRestoreError = error{
        Canceled,
        DeadlineExceeded,
        NotLeader,
        TableAlreadyExists,
        MethodNotAllowed,
        BackupManifestTooLarge,
        UnsupportedBackupMigrationState,
        UnsupportedMultiRangeTable,
        UnsupportedBackupFormat,
        RestoreValidationPending,
        RestoreDurabilityPending,
        RestoreDurabilityConfirmed,
        BackupIntegrityFailure,
        RestoreDestinationReauthorizationRequired,
        UnsupportedArtifactIndexSources,
        ArtifactIndexSourcesTemporarilyUnavailable,
        InvalidBackupRequest,
        InternalFailure,
    };

    pub const ExecuteListIndexesError = error{
        Canceled,
        DeadlineExceeded,
        NotFound,
        InternalFailure,
    };

    pub const ExecuteGetIndexError = error{
        Canceled,
        DeadlineExceeded,
        NotFound,
        InternalFailure,
    };

    pub const ExecuteCreateIndexError = error{
        Canceled,
        DeadlineExceeded,
        NotLeader,
        NotFound,
        Conflict,
        MethodNotAllowed,
        InvalidIndexRequest,
        UnsupportedArtifactIndexSources,
        ArtifactIndexSourcesTemporarilyUnavailable,
        ProbeUnavailable,
        ModelNotFound,
        Backpressured,
        InternalFailure,
    };

    pub const ExecuteDeleteIndexError = error{
        Canceled,
        DeadlineExceeded,
        NotLeader,
        NotFound,
        Conflict,
        MethodNotAllowed,
        InternalFailure,
    };

    pub const ExecutePutArtifactEnrichmentError = error{
        Canceled,
        DeadlineExceeded,
        NotLeader,
        NotFound,
        Conflict,
        MethodNotAllowed,
        InvalidEnrichmentRequest,
        InternalFailure,
    };

    pub const ExecuteDeleteArtifactEnrichmentError = error{
        Canceled,
        DeadlineExceeded,
        NotLeader,
        NotFound,
        Conflict,
        MethodNotAllowed,
        InvalidEnrichmentRequest,
        InternalFailure,
    };

    pub const ExecuteListArtifactEnrichmentsError = error{
        Canceled,
        DeadlineExceeded,
        NotFound,
        InternalFailure,
    };

    pub const ExecuteDocumentArtifactManifestError = error{
        Canceled,
        DeadlineExceeded,
        NotFound,
        MethodNotAllowed,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        StorageReadTemporarilyUnavailable,
        InternalFailure,
    };

    pub const ExecuteDocumentArtifactManifestsError = error{
        Canceled,
        DeadlineExceeded,
        NotFound,
        MethodNotAllowed,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        StorageReadTemporarilyUnavailable,
        InternalFailure,
    };

    pub const ExecuteReprocessDocumentArtifactError = error{
        Canceled,
        DeadlineExceeded,
        NotFound,
        MethodNotAllowed,
        DocIdentityUnavailable,
        InternalFailure,
    };

    pub const ExecuteReprocessDocumentArtifactRangeError = error{
        Canceled,
        DeadlineExceeded,
        NotFound,
        MethodNotAllowed,
        InvalidRequest,
        DocIdentityUnavailable,
        InternalFailure,
    };

    pub const TableQueryView = enum {
        default_view,
        published,
        latest,
    };

    pub const VTable = struct {
        execute_table_batch: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
            request: operation.RequestContext,
        ) ExecuteBatchError!void,
        execute_table_query_request: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            body: []const u8,
            row_filter_json: ?[]const u8,
            request: operation.RequestContext,
        ) ExecuteQueryError![]u8,
        execute_table_query_view: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            view: TableQueryView,
            request: operation.RequestContext,
        ) ExecuteQueryViewError![]u8,
        execute_table_backup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            backup_id: []const u8,
            format: backups_api.BackupFormat,
            expected_fence: ?backups_api.TableBackupFence,
            location_uri: []const u8,
            connection: []const u8,
            location: *backups_api.BackupLocation,
            request: operation.RequestContext,
        ) ExecuteBackupError!void,
        execute_table_restore: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            backup_id: []const u8,
            location_uri: []const u8,
            connection: []const u8,
            location: *backups_api.BackupLocation,
            request: operation.RequestContext,
        ) ExecuteRestoreError!void,
        execute_table_list_indexes: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            request: operation.RequestContext,
        ) ExecuteListIndexesError![]u8,
        execute_table_get_index: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            request: operation.RequestContext,
        ) ExecuteGetIndexError![]u8,
        execute_table_create_index: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            body: []const u8,
            request: operation.RequestContext,
        ) ExecuteCreateIndexError![]u8,
        execute_table_delete_index: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            request: operation.RequestContext,
        ) ExecuteDeleteIndexError!void,
        execute_put_artifact_enrichment: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            body: []const u8,
            request: operation.RequestContext,
        ) ExecutePutArtifactEnrichmentError!void = null,
        execute_delete_artifact_enrichment: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            request: operation.RequestContext,
        ) ExecuteDeleteArtifactEnrichmentError!void = null,
        execute_list_artifact_enrichments: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            request: operation.RequestContext,
        ) ExecuteListArtifactEnrichmentsError![]u8 = null,
        execute_document_artifact_manifest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            request: operation.RequestContext,
        ) ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest = null,
        execute_document_artifact_manifests: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            request: operation.RequestContext,
        ) ExecuteDocumentArtifactManifestsError!db_mod.types.DocumentArtifactManifestList = null,
        execute_reprocess_document_artifact: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            request: operation.RequestContext,
        ) ExecuteReprocessDocumentArtifactError!void = null,
        execute_reprocess_document_artifact_range: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            req: db_mod.types.DocumentArtifactTableReprocessRequest,
            request: operation.RequestContext,
        ) ExecuteReprocessDocumentArtifactRangeError!db_mod.types.DocumentArtifactTableReprocessResult = null,
    };

    pub fn executeTableBatch(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) ExecuteBatchError!void {
        try self.ensureActive();
        return try self.vtable.execute_table_batch(self.ptr, alloc, table_name, req, self.request);
    }

    pub fn executeTableQueryRequest(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
    ) ExecuteQueryError![]u8 {
        try self.ensureActive();
        return try self.vtable.execute_table_query_request(self.ptr, alloc, table_name, body, row_filter_json, self.request);
    }

    pub fn executeTableQueryView(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        view: TableQueryView,
    ) ExecuteQueryViewError![]u8 {
        try self.ensureActive();
        return try self.vtable.execute_table_query_view(self.ptr, alloc, table_name, view, self.request);
    }

    pub fn executeTableBackup(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        format: backups_api.BackupFormat,
        expected_fence: ?backups_api.TableBackupFence,
        location_uri: []const u8,
        connection: []const u8,
        location: *backups_api.BackupLocation,
    ) ExecuteBackupError!void {
        try self.ensureActive();
        return try self.vtable.execute_table_backup(self.ptr, alloc, table_name, backup_id, format, expected_fence, location_uri, connection, location, self.request);
    }

    pub fn executeTableRestore(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        location_uri: []const u8,
        connection: []const u8,
        location: *backups_api.BackupLocation,
    ) ExecuteRestoreError!void {
        try self.ensureActive();
        return try self.vtable.execute_table_restore(self.ptr, alloc, table_name, backup_id, location_uri, connection, location, self.request);
    }

    pub fn executeTableListIndexes(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) ExecuteListIndexesError![]u8 {
        try self.ensureActive();
        return try self.vtable.execute_table_list_indexes(self.ptr, alloc, table_name, self.request);
    }

    pub fn executeTableGetIndex(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) ExecuteGetIndexError![]u8 {
        try self.ensureActive();
        return try self.vtable.execute_table_get_index(self.ptr, alloc, table_name, index_name, self.request);
    }

    pub fn executeTableCreateIndex(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        body: []const u8,
    ) ExecuteCreateIndexError![]u8 {
        try self.ensureActive();
        return try self.vtable.execute_table_create_index(self.ptr, alloc, table_name, index_name, body, self.request);
    }

    pub fn executeTableDeleteIndex(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) ExecuteDeleteIndexError!void {
        try self.ensureActive();
        return try self.vtable.execute_table_delete_index(self.ptr, alloc, table_name, index_name, self.request);
    }

    pub fn executePutArtifactEnrichment(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        body: []const u8,
    ) ExecutePutArtifactEnrichmentError!void {
        const fn_ptr = self.vtable.execute_put_artifact_enrichment orelse return error.MethodNotAllowed;
        try self.ensureActive();
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name, body, self.request);
    }

    pub fn executeDeleteArtifactEnrichment(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
    ) ExecuteDeleteArtifactEnrichmentError!void {
        const fn_ptr = self.vtable.execute_delete_artifact_enrichment orelse return error.MethodNotAllowed;
        try self.ensureActive();
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name, self.request);
    }

    pub fn executeListArtifactEnrichments(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) ExecuteListArtifactEnrichmentsError![]u8 {
        const fn_ptr = self.vtable.execute_list_artifact_enrichments orelse return error.NotFound;
        try self.ensureActive();
        return try fn_ptr(self.ptr, alloc, table_name, self.request);
    }

    pub fn executeDocumentArtifactManifest(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest {
        const fn_ptr = self.vtable.execute_document_artifact_manifest orelse return error.MethodNotAllowed;
        try self.ensureActive();
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, artifact_name, self.request);
    }

    pub fn executeDocumentArtifactManifests(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
    ) ExecuteDocumentArtifactManifestsError!db_mod.types.DocumentArtifactManifestList {
        const fn_ptr = self.vtable.execute_document_artifact_manifests orelse return error.MethodNotAllowed;
        try self.ensureActive();
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, self.request);
    }

    pub fn executeReprocessDocumentArtifact(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) ExecuteReprocessDocumentArtifactError!void {
        const fn_ptr = self.vtable.execute_reprocess_document_artifact orelse return error.MethodNotAllowed;
        try self.ensureActive();
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, artifact_name, self.request);
    }

    pub fn executeReprocessDocumentArtifactRange(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) ExecuteReprocessDocumentArtifactRangeError!db_mod.types.DocumentArtifactTableReprocessResult {
        const fn_ptr = self.vtable.execute_reprocess_document_artifact_range orelse return error.MethodNotAllowed;
        try self.ensureActive();
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name, req, self.request);
    }
};

pub const testing = if (builtin.is_test) struct {
    pub fn hasInternalShardQueryFields(alloc: std.mem.Allocator, body: []const u8) !bool {
        return bodyHasInternalShardQueryFields(alloc, body);
    }
} else struct {};

pub const OwnedResponse = struct {
    status: u16,
    body: []u8,
    json: bool = false,
    retry_after_seconds: ?u32 = null,

    pub fn deinit(self: *OwnedResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const storage_read_temporarily_unavailable_body = "{\"code\":\"storage_read_temporarily_unavailable\",\"message\":\"storage read temporarily unavailable\",\"retryable\":true}";
pub const storage_read_temporarily_unavailable_retry_after_seconds: u32 = 1;

/// Stable, machine-readable reasons for a retryable query 503. Keep this set in
/// sync with QueryTemporarilyUnavailableError in the public OpenAPI contract.
pub const QueryTemporarilyUnavailableReason = enum {
    doc_identity_unavailable,
    read_requires_primary,
    standby_read_unavailable,
    storage_read_temporarily_unavailable,
    index_rebuilding,
    query_embedding_temporarily_unavailable,
};

pub fn queryTemporarilyUnavailableOwnedResponse(
    alloc: std.mem.Allocator,
    reason: QueryTemporarilyUnavailableReason,
) !OwnedResponse {
    const message: []const u8 = switch (reason) {
        .doc_identity_unavailable => "doc identity unavailable",
        .read_requires_primary => "read requires primary",
        .standby_read_unavailable => "standby read unavailable",
        .storage_read_temporarily_unavailable => "storage read temporarily unavailable",
        .index_rebuilding => "required index is rebuilding",
        .query_embedding_temporarily_unavailable => "query embedding temporarily unavailable",
    };
    return .{
        .status = 503,
        .body = try std.json.Stringify.valueAlloc(alloc, .{
            .code = @tagName(reason),
            .message = message,
            .retryable = true,
        }, .{}),
        .json = true,
        .retry_after_seconds = storage_read_temporarily_unavailable_retry_after_seconds,
    };
}

pub fn storageReadTemporarilyUnavailableOwnedResponse(alloc: std.mem.Allocator) !OwnedResponse {
    return queryTemporarilyUnavailableOwnedResponse(alloc, .storage_read_temporarily_unavailable);
}

fn expectQueryTemporarilyUnavailableResponse(
    alloc: std.mem.Allocator,
    response: OwnedResponse,
    code: []const u8,
    message: []const u8,
) !void {
    const expected = try std.json.Stringify.valueAlloc(alloc, .{
        .code = code,
        .message = message,
        .retryable = true,
    }, .{});
    defer alloc.free(expected);
    try ant_json.testing.expectSubsetJsonText(alloc, expected, response.body);
    try std.testing.expect(response.json);
    try std.testing.expectEqual(@as(?u32, 1), response.retry_after_seconds);
}

pub fn hierarchyCursorStaleBody(alloc: std.mem.Allocator) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = 409,
        .@"error" = "hierarchy_cursor_stale",
        .message = "the source hierarchy changed after this cursor was issued",
        .action = "restart_hierarchy_traversal",
        .restart_without = "search_after",
        .retryable = false,
    }, .{});
}

pub fn topologyChangedBody(alloc: std.mem.Allocator) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = 409,
        .@"error" = "topology_changed",
        .message = "the table topology changed while the query was running",
        .action = "retry_query",
        .retryable = true,
    }, .{});
}

/// Stable, non-retryable public error for hierarchy grouping that cannot be
/// represented because at least one selected member lacks durable unit
/// identity. Keep this in
/// sync with UnsupportedHierarchyGroupingError in the public OpenAPI contract.
pub const UnsupportedHierarchyGroupingError = struct {
    status: u16 = 422,
    @"error": []const u8 = "unsupported_hierarchy_grouping",
    message: []const u8 = "the selected index contains members without durable unit identity; use hierarchy.group_by.level=source, omit hierarchy.group_by for direct members, or query an index whose every source is unit-backed",
    reason: []const u8 = "unit_identity_unavailable",
    field: []const u8 = "hierarchy.group_by.level",
    action: []const u8 = "use_source_grouping_or_direct_members",
    retryable: bool = false,
};

pub fn unsupportedHierarchyGroupingBody(alloc: std.mem.Allocator) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, UnsupportedHierarchyGroupingError{}, .{});
}

/// Stable fallback for unsupported queries that do not have a more specific
/// structured diagnostic. Keep this in sync with UnsupportedQueryError in the
/// public OpenAPI contract.
pub const UnsupportedQueryError = struct {
    status: u16 = 422,
    @"error": []const u8 = "unsupported_query_request",
    message: []const u8 = "unsupported query request",
    retryable: bool = false,
};

pub fn unsupportedQueryBody(alloc: std.mem.Allocator) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, UnsupportedQueryError{}, .{});
}

pub fn isNonRetryableTableStorageReadError(err: anyerror) bool {
    return switch (err) {
        error.InvalidManifest,
        error.InvalidTableFile,
        error.TableBlockChecksumMismatch,
        error.CorruptInput,
        error.UnsupportedVersion,
        error.Corrupted,
        => true,
        else => false,
    };
}

pub fn tableStorageUnreadableBody(alloc: std.mem.Allocator, err: anyerror) ![]u8 {
    std.debug.assert(isNonRetryableTableStorageReadError(err));
    return try std.json.Stringify.valueAlloc(alloc, .{
        .code = "table_storage_unreadable",
        .@"error" = @errorName(err),
        .message = "table storage unreadable",
        .retryable = false,
    }, .{});
}

fn unsupportedExactSortBody(alloc: std.mem.Allocator) ![]u8 {
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse db_mod.SortRejectionDiagnostic{};
    const public_rejection = query_contract.publicExactSortRejection(diagnostic.reason, diagnostic.detail);
    return try std.json.Stringify.valueAlloc(alloc, struct {
        @"error": []const u8 = "unsupported_exact_sort",
        message: []const u8 = "exact sort is unsupported for this query",
        reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
        status: u16 = 422,
    }{
        .reason = public_rejection.reason,
        .sort_rejection_reason = public_rejection.reason,
        .sort_rejection_detail = public_rejection.detail,
        .sort_rejection_field = diagnostic.field,
    }, .{});
}

fn queryCandidateBudgetExceededBody(alloc: std.mem.Allocator) ![]u8 {
    const diagnostic = db_mod.takeLastSortRejectionDiagnostic() orelse db_mod.SortRejectionDiagnostic{
        .reason = "candidate_budget_exceeded",
        .detail = "candidate_budget_exceeded",
    };
    const public_rejection = query_contract.publicExactSortRejection(diagnostic.reason, diagnostic.detail);
    return try std.json.Stringify.valueAlloc(alloc, struct {
        @"error": []const u8 = "query_candidate_budget_exceeded",
        message: []const u8 = "query candidate budget exceeded",
        reason: []const u8,
        budget_rejection_reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
        status: u16 = 422,
    }{
        .reason = public_rejection.reason,
        .budget_rejection_reason = diagnostic.detail,
        .sort_rejection_reason = public_rejection.reason,
        .sort_rejection_detail = public_rejection.detail,
        .sort_rejection_field = diagnostic.field,
    }, .{});
}

pub fn graphWorkBudgetExceededBody(alloc: std.mem.Allocator) ![]u8 {
    const diagnostic = graph_work_budget_diagnostic.take() orelse graph_work_budget_diagnostic.Diagnostic{
        .operation = "$request",
        .mode = "graph_queries",
        .dimension = .explored_edges,
        .maximum = graph_pattern_mod.default_max_explored_edges,
    };
    const dimension = graph_work_budget_diagnostic.dimensionName(diagnostic.dimension);
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = @as(u16, 422),
        .@"error" = "graph_work_budget_exceeded",
        .message = "exact graph execution exceeded its bounded work budget",
        .retryable = false,
        .operation = diagnostic.operation,
        .mode = diagnostic.mode,
        .dimension = dimension,
        .maximum = diagnostic.maximum,
        .remediation = "narrow the operation's anchor/filter, reduce path breadth or depth, or split the query",
    }, .{});
}

pub fn graphPathWeightDomainErrorBody(alloc: std.mem.Allocator) ![]u8 {
    const diagnostic = graph_path_weight_diagnostic.take() orelse
        return error.MissingGraphPathWeightDiagnostic;
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = @as(u16, 422),
        .@"error" = "graph_path_weight_domain_error",
        .message = "an edge weight or accumulated path score is outside the path algorithm's exact numeric domain",
        .retryable = false,
        .operation = diagnostic.operation,
        .objective = @tagName(diagnostic.objective),
        .violation = @tagName(diagnostic.violation),
        .remediation = "normalize edge weights or select a compatible path weight mode",
    }, .{});
}

test "graph path weight error body fails closed without its diagnostic" {
    var storage: graph_path_weight_diagnostic.Storage = .{};
    const binding = graph_path_weight_diagnostic.bind(&storage);
    defer binding.deinit();
    graph_path_weight_diagnostic.reset();
    try std.testing.expectError(
        error.MissingGraphPathWeightDiagnostic,
        graphPathWeightDomainErrorBody(std.testing.allocator),
    );
}

pub fn graphDistinctBudgetExceededBody(alloc: std.mem.Allocator) ![]u8 {
    const diagnostic = graph_distinct_budget_diagnostic.take() orelse
        return error.MissingGraphDistinctBudgetDiagnostic;
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = @as(u16, 422),
        .@"error" = "graph_distinct_budget_exceeded",
        .message = "exact graph distinct aggregation exceeded its request budget",
        .retryable = false,
        .operation = diagnostic.operation,
        .dimension = graph_distinct_budget_diagnostic.dimensionName(diagnostic.dimension),
        .maximum = diagnostic.maximum,
        .remediation = "narrow match.anchor, reduce matching cardinality, split the query, or remove distinct",
    }, .{});
}

pub fn graphAnchorFilterRequiresIndexBody(alloc: std.mem.Allocator) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = @as(u16, 422),
        .@"error" = "graph_anchor_filter_requires_index",
        .message = "exact graph anchor enumeration requires native index coverage for stored-field and authorization filters; ids-only filters use the primary identity index",
        .retryable = false,
    }, .{});
}

const GraphQueryUnsupportedDiagnostic = struct {
    operation: []const u8 = "$request",
    feature: []const u8 = "graph_queries",
    reason: []const u8 = "unsupported_mode",
};

pub fn graphQueryUnsupportedBody(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    var parsed = ant_json.parseFromSlice(std.json.Value, alloc, body, .{}) catch null;
    defer if (parsed) |*value| value.deinit();
    var diagnostic = if (parsed) |value| graphQueryUnsupportedDiagnostic(value.value) else GraphQueryUnsupportedDiagnostic{};
    if (graph_query_diagnostic.take()) |recorded| {
        diagnostic = .{
            .operation = recorded.operation,
            .feature = recorded.feature,
            .reason = @tagName(recorded.reason),
        };
    }
    return try graphQueryUnsupportedBodyForDiagnostic(alloc, diagnostic);
}

pub fn graphQueryCapabilityUnsupportedBody(
    alloc: std.mem.Allocator,
    body: []const u8,
    reason: ?[]const u8,
) ![]u8 {
    var parsed = ant_json.parseFromSlice(std.json.Value, alloc, body, .{}) catch null;
    defer if (parsed) |*value| value.deinit();
    var diagnostic = if (parsed) |value| graphQueryUnsupportedDiagnostic(value.value) else GraphQueryUnsupportedDiagnostic{};
    if (reason) |value| {
        if (graph_query_diagnostic.take()) |recorded| {
            if (std.mem.eql(u8, value, @tagName(recorded.reason))) {
                diagnostic = .{
                    .operation = recorded.operation,
                    .feature = recorded.feature,
                    .reason = value,
                };
            } else {
                diagnostic.reason = value;
            }
        } else {
            diagnostic.reason = value;
        }
    }
    return try graphQueryUnsupportedBodyForDiagnostic(alloc, diagnostic);
}

fn graphQueryUnsupportedBodyForDiagnostic(
    alloc: std.mem.Allocator,
    diagnostic: GraphQueryUnsupportedDiagnostic,
) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = @as(u16, 422),
        .@"error" = "graph_query_unsupported",
        .message = graphQueryUnsupportedMessage(diagnostic.reason),
        .retryable = false,
        .operation = diagnostic.operation,
        .feature = diagnostic.feature,
        .reason = diagnostic.reason,
    }, .{});
}

fn graphQueryUnsupportedMessage(reason: []const u8) []const u8 {
    if (std.mem.eql(u8, reason, "expand_strategy_not_supported"))
        return "expand_strategy is a legacy graph_searches result-merging control; remove it when using graph_queries and consume the named typed graph_results directly";
    if (std.mem.eql(u8, reason, "legacy_graph_searches_not_supported"))
        return "serverless graph queries require graph_queries; graph_searches is available only on stateful/provisioned Antfly during its compatibility window";
    if (std.mem.eql(u8, reason, "request_control_not_supported"))
        return "this request control cannot be combined with exact graph execution in this runtime; remove the field or run the graph query separately";
    if (std.mem.eql(u8, reason, "external_alias_document_filter_not_supported"))
        return "this runtime snapshot cannot evaluate stored-document filters on aliases outside the queried table; remove that alias filter or use coordinator-backed execution";
    if (std.mem.eql(u8, reason, "external_alias_source_not_supported"))
        return "this runtime snapshot cannot expand relationships from an alias outside the queried table; use coordinator-backed execution or express table boundaries as terminal aliases";
    if (std.mem.eql(u8, reason, "reverse_variable_path_not_supported"))
        return "exact execution cannot prove a planner-required reverse variable-length expansion; use explicit single-hop relationships at table boundaries";
    return "this graph query mode is not supported by exact public execution; use graph_queries with outgoing, deduplicated traversal semantics";
}

fn graphQueryUnsupportedDiagnostic(root: std.json.Value) GraphQueryUnsupportedDiagnostic {
    if (root != .object) return .{};
    if (root.object.get("expand_strategy")) |value| {
        if (value != .null) return .{
            .feature = "expand_strategy",
            .reason = "expand_strategy_not_supported",
        };
    }
    if (root.object.get("graph_queries")) |queries| {
        if (queries == .object) return canonicalGraphUnsupportedDiagnostic(queries.object);
    }
    if (root.object.get("graph_searches")) |queries| {
        if (queries == .object) return legacyGraphUnsupportedDiagnostic(queries.object);
    }
    return .{};
}

fn canonicalGraphUnsupportedDiagnostic(queries: std.json.ObjectMap) GraphQueryUnsupportedDiagnostic {
    var fallback: ?GraphQueryUnsupportedDiagnostic = null;
    var it = queries.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const operation_name = entry.key_ptr.*;
        const value = entry.value_ptr.object;
        const mode = canonicalGraphMode(value) orelse "unknown";
        if (fallback == null) fallback = .{ .operation = operation_name, .feature = mode };
        const params = value.get(mode) orelse continue;
        if (params != .object) continue;
        if (jsonString(params.object, "direction")) |direction| {
            if (!std.mem.eql(u8, direction, "out")) return .{
                .operation = operation_name,
                .feature = mode,
                .reason = "direction_must_be_out",
            };
        }
        if (std.mem.eql(u8, mode, "traverse") and jsonBool(params.object, "deduplicate_nodes") == false) {
            return .{
                .operation = operation_name,
                .feature = mode,
                .reason = "deduplicate_nodes_must_be_true",
            };
        }
        if (params.object.get("start")) |selector| {
            if (selectorRefUnsupported(selector)) return .{
                .operation = operation_name,
                .feature = mode,
                .reason = "start_selector_not_supported",
            };
        }
    }
    return fallback orelse .{};
}

fn legacyGraphUnsupportedDiagnostic(queries: std.json.ObjectMap) GraphQueryUnsupportedDiagnostic {
    var fallback: ?GraphQueryUnsupportedDiagnostic = null;
    var it = queries.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const operation_name = entry.key_ptr.*;
        const value = entry.value_ptr.object;
        const mode = jsonString(value, "type") orelse "unknown";
        if (fallback == null) fallback = .{ .operation = operation_name, .feature = mode };
        if (value.get("params")) |params| {
            if (params == .object) {
                if (jsonString(params.object, "direction")) |direction| {
                    if (!std.mem.eql(u8, direction, "out")) return .{
                        .operation = operation_name,
                        .feature = mode,
                        .reason = "direction_must_be_out",
                    };
                }
                if (jsonBool(params.object, "deduplicate_nodes") == false) return .{
                    .operation = operation_name,
                    .feature = mode,
                    .reason = "deduplicate_nodes_must_be_true",
                };
            }
        }
        if (value.get("start_nodes")) |selector| {
            if (selectorRefUnsupported(selector)) return .{
                .operation = operation_name,
                .feature = mode,
                .reason = "start_selector_not_supported",
            };
        }
        if (value.get("target_nodes")) |selector| {
            if (selectorRefUnsupported(selector)) return .{
                .operation = operation_name,
                .feature = mode,
                .reason = "target_selector_not_supported",
            };
        }
        if (value.get("params")) |params| {
            if (params == .object) {
                if (std.mem.eql(u8, mode, "neighbors") or std.mem.eql(u8, mode, "traverse")) {
                    if (jsonString(params.object, "weight_mode")) |weight_mode| {
                        if (!std.mem.eql(u8, weight_mode, "min_hops")) return .{
                            .operation = operation_name,
                            .feature = mode,
                            .reason = "weight_mode_must_be_min_hops",
                        };
                    }
                }
                if (std.mem.eql(u8, mode, "shortest_path")) {
                    if (jsonInteger(params.object, "k")) |k| {
                        if (k != 1) return .{
                            .operation = operation_name,
                            .feature = mode,
                            .reason = "k_must_equal_one",
                        };
                    }
                }
            }
        }
        if ((std.mem.eql(u8, mode, "shortest_path") or std.mem.eql(u8, mode, "k_shortest_paths")) and
            value.get("target_nodes") == null)
        {
            return .{ .operation = operation_name, .feature = mode, .reason = "target_required" };
        }
        if (std.mem.eql(u8, mode, "pattern")) {
            const pattern = value.get("pattern");
            if (pattern == null or pattern.? != .array or pattern.?.array.items.len == 0) return .{
                .operation = operation_name,
                .feature = mode,
                .reason = "pattern_required",
            };
            for (pattern.?.array.items) |step| {
                if (step != .object) continue;
                const edge = step.object.get("edge") orelse continue;
                if (edge != .object) continue;
                if (jsonString(edge.object, "direction")) |direction| {
                    if (!std.mem.eql(u8, direction, "out")) return .{
                        .operation = operation_name,
                        .feature = mode,
                        .reason = "direction_must_be_out",
                    };
                }
            }
        }
    }
    return fallback orelse .{};
}

fn canonicalGraphMode(value: std.json.ObjectMap) ?[]const u8 {
    inline for ([_][]const u8{ "match", "traverse", "shortest_path", "k_shortest_paths" }) |mode| {
        if (value.get(mode) != null) return mode;
    }
    return null;
}

fn jsonString(value: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const item = value.get(key) orelse return null;
    return if (item == .string) item.string else null;
}

fn jsonBool(value: std.json.ObjectMap, key: []const u8) ?bool {
    const item = value.get(key) orelse return null;
    return if (item == .bool) item.bool else null;
}

fn jsonInteger(value: std.json.ObjectMap, key: []const u8) ?i64 {
    const item = value.get(key) orelse return null;
    return if (item == .integer) item.integer else null;
}

fn selectorRefUnsupported(selector: std.json.Value) bool {
    if (selector != .object) return false;
    const result_ref = jsonString(selector.object, "result_ref") orelse return false;
    if (std.mem.eql(u8, result_ref, "$query_results")) return false;
    return !std.mem.startsWith(u8, result_ref, "$graph_results.") or result_ref.len == "$graph_results.".len;
}

fn graphMatchOperationCountFromBody(alloc: std.mem.Allocator, body: []const u8) usize {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return graph_query_mod.max_match_queries_per_request + 1;
    defer parsed.deinit();
    if (parsed.value != .object) return graph_query_mod.max_match_queries_per_request + 1;
    if (parsed.value.object.get("graph_queries")) |queries| {
        if (queries != .object) return graph_query_mod.max_match_queries_per_request + 1;
        var count: usize = 0;
        var it = queries.object.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* == .object and entry.value_ptr.object.get("match") != null) count += 1;
        }
        return count;
    }
    const queries = parsed.value.object.get("graph_searches") orelse return graph_query_mod.max_match_queries_per_request + 1;
    if (queries != .object) return graph_query_mod.max_match_queries_per_request + 1;
    var count: usize = 0;
    var it = queries.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const query_type = entry.value_ptr.object.get("type") orelse continue;
        if (query_type == .string and std.mem.eql(u8, query_type.string, "pattern")) count += 1;
    }
    return count;
}

pub fn graphMatchOperationLimitExceededBody(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, .{
        .status = @as(u16, 422),
        .@"error" = "graph_match_operation_limit_exceeded",
        .message = "too many named MATCH operations; put multiple aggregates over one pattern in the same MATCH return object",
        .retryable = false,
        .maximum = graph_query_mod.max_match_queries_per_request,
        .actual = graphMatchOperationCountFromBody(alloc, body),
    }, .{});
}

pub fn handleTableBatch(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    api: TableApi,
) !OwnedResponse {
    resetLastBatchFailureName();
    var batch_req = batch_api.parseBatchRequest(alloc, body) catch |err| {
        switch (err) {
            error.ValueTooLong => return .{ .status = 413, .body = try alloc.dupe(u8, "value too large") },
            error.InvalidBatchRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid batch request") },
            else => return err,
        }
    };
    defer batch_req.deinit(alloc);

    api.executeTableBatch(alloc, table_name, batch_req.req) catch |err| switch (err) {
        error.InvalidBatchRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid batch request") },
        error.UnsupportedSyncLevel => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported sync_level") },
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.Conflict => return .{ .status = 409, .body = try alloc.dupe(u8, "batch transaction conflicted") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.Backpressured => return .{ .status = 429, .body = try alloc.dupe(u8, "table backpressured") },
        error.DenseRepairBackpressure => return .{
            .status = 429,
            .body = try alloc.dupe(u8, "{\"code\":\"dense_repair_backpressure\",\"message\":\"writes are temporarily limited while a dense index rebuild catches up\",\"retryable\":true,\"retry_after_ms\":1000}"),
            .json = true,
            .retry_after_seconds = 1,
        },
        error.Unavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "maintenance routes unavailable on query-only runtime") },
        error.WriteUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "write unavailable") },
        error.HAWriteDurabilityPending => return .{
            .status = 503,
            .body = try alloc.dupe(u8, "write committed locally; standby durability acknowledgment pending"),
        },
        error.OutcomeUnknown => return .{
            .status = 500,
            .body = try alloc.dupe(u8, "transaction outcome is unknown; do not retry this stateless batch because it may already have committed; use a transaction session for retryable commits"),
        },
        error.CommittedPending => return .{
            .status = 202,
            .body = try batch_api.encodeBatchResponse(alloc, batch_req.resultWithStatus("committed_pending")),
            .json = true,
        },
        error.CommittedRepairRequired => return .{
            .status = 202,
            .body = try batch_api.encodeBatchResponse(alloc, batch_req.resultWithStatus("committed_repair_required")),
            .json = true,
        },
        // Do not use a retryable 5xx: clients must reconcile an ambiguous
        // commit result instead of blindly replaying non-idempotent transforms.
        error.WriteOutcomeUnknown => return .{ .status = 409, .body = try alloc.dupe(u8, "write outcome unknown") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.HAReadOnlyStandby => return .{ .status = 409, .body = try alloc.dupe(u8, "standby is read-only") },
        error.HAPromotedStandbyRequiresPrimaryOpen => return .{ .status = 409, .body = try alloc.dupe(u8, "promoted standby requires primary open") },
        error.HAFencedPrimary => return .{ .status = 409, .body = try alloc.dupe(u8, "fenced primary rejects writes") },
        error.Canceled => return error.Canceled,
        error.DeadlineExceeded => return error.DeadlineExceeded,
        error.InternalFailure => {
            const error_name = takeLastBatchFailureName() orelse @errorName(error.InternalFailure);
            return .{
                .status = 500,
                .body = try std.json.Stringify.valueAlloc(alloc, .{
                    .@"error" = error_name,
                    .message = "batch failed",
                }, .{}),
                .json = true,
            };
        },
    };

    return .{
        .status = 201,
        .body = try batch_api.encodeBatchResponse(alloc, batch_req.result()),
        .json = true,
    };
}

pub fn handleTableQueryRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    row_filter_json: ?[]const u8,
    api: TableApi,
) !OwnedResponse {
    var diagnostic_context: graph_request_diagnostics.Context = .{};
    const diagnostic_scope = graph_request_diagnostics.Scope.init(&diagnostic_context);
    defer diagnostic_scope.deinit();

    if (try bodyHasInternalShardQueryFields(alloc, body)) {
        std.log.warn("public table query rejected internal fields table={s}", .{table_name});
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid query request") };
    }

    db_mod.resetLastSortRejectionDiagnostic();
    graph_query_diagnostic.reset();
    graph_distinct_budget_diagnostic.reset();
    graph_work_budget_diagnostic.reset();
    graph_path_weight_diagnostic.reset();
    query_contract.validatePublicQuerySortTupleContract(alloc, body) catch |err| switch (err) {
        error.InvalidQueryRequest => {
            std.log.warn("public table query invalid exact sort table={s} err={}", .{ table_name, err });
            return .{ .status = 422, .body = try unsupportedExactSortBody(alloc), .json = true };
        },
    };
    const response_body = api.executeTableQueryRequest(alloc, table_name, body, row_filter_json) catch |err| {
        switch (err) {
            error.InvalidQueryRequest => {
                if (db_mod.peekLastSortRejectionDiagnostic() != null) {
                    std.log.warn("public table query invalid exact sort table={s} err={}", .{ table_name, err });
                    return .{ .status = 422, .body = try unsupportedExactSortBody(alloc), .json = true };
                }
                std.log.err("public table query invalid table={s} err={}", .{ table_name, err });
                return .{ .status = 400, .body = try alloc.dupe(u8, "invalid query request") };
            },
            error.InvalidFilterQueryRequest => return publicFilterQueryErrorResponse(
                alloc,
                body,
                "filter_query",
                .invalid,
            ),
            error.InvalidExclusionQueryRequest => return publicFilterQueryErrorResponse(
                alloc,
                body,
                "exclusion_query",
                .invalid,
            ),
            error.UnsupportedFilterQueryRequest => return publicFilterQueryErrorResponse(
                alloc,
                body,
                "filter_query",
                .unsupported,
            ),
            error.UnsupportedExclusionQueryRequest => return publicFilterQueryErrorResponse(
                alloc,
                body,
                "exclusion_query",
                .unsupported,
            ),
            error.UnsupportedQueryRequest => return .{
                .status = 422,
                .body = try unsupportedQueryBody(alloc),
                .json = true,
            },
            error.UnsupportedHierarchyGrouping => return .{
                .status = 422,
                .body = try unsupportedHierarchyGroupingBody(alloc),
                .json = true,
            },
            error.NotFound => {
                std.log.err("public table query missing table={s} err={}", .{ table_name, err });
                return .{ .status = 404, .body = try alloc.dupe(u8, "not found") };
            },
            error.DocIdentityUnavailable => {
                std.log.warn("public table query doc identity unavailable table={s} err={}", .{ table_name, err });
                return try queryTemporarilyUnavailableOwnedResponse(alloc, .doc_identity_unavailable);
            },
            error.ReadRequiresPrimary => {
                std.log.warn("public table query requires primary table={s} err={}", .{ table_name, err });
                return try queryTemporarilyUnavailableOwnedResponse(alloc, .read_requires_primary);
            },
            error.ReadUnavailable => {
                std.log.warn("public table query standby unavailable table={s} err={}", .{ table_name, err });
                return try queryTemporarilyUnavailableOwnedResponse(alloc, .standby_read_unavailable);
            },
            error.StorageReadTemporarilyUnavailable => {
                std.log.warn("public table query storage temporarily unavailable table={s}", .{table_name});
                return try storageReadTemporarilyUnavailableOwnedResponse(alloc);
            },
            error.IndexRebuilding => {
                std.log.info("public table query index rebuilding table={s}", .{table_name});
                return try queryTemporarilyUnavailableOwnedResponse(alloc, .index_rebuilding);
            },
            error.ModelNotFound => {
                std.log.warn("public table query model not found table={s} err={}", .{ table_name, err });
                return .{ .status = 404, .body = try alloc.dupe(u8, "{\"error\":\"MODEL_NOT_FOUND\",\"message\":\"model not found\"}") };
            },
            error.QueryCandidateBudgetExceeded => {
                std.log.warn("public table query candidate budget exceeded table={s} err={}", .{ table_name, err });
                return .{ .status = 422, .body = try queryCandidateBudgetExceededBody(alloc), .json = true };
            },
            error.GraphWorkBudgetExceeded => {
                std.log.warn("public table graph work budget exceeded table={s}", .{table_name});
                return .{ .status = 422, .body = try graphWorkBudgetExceededBody(alloc), .json = true };
            },
            error.GraphMinWeightDomainViolation, error.GraphMaxWeightDomainViolation, error.GraphPathWeightOverflow => {
                std.log.warn("public table graph path weight domain violation table={s}", .{table_name});
                return .{ .status = 422, .body = try graphPathWeightDomainErrorBody(alloc), .json = true };
            },
            error.GraphDistinctBudgetExceeded => {
                std.log.warn("public table graph distinct budget exceeded table={s}", .{table_name});
                return .{ .status = 422, .body = try graphDistinctBudgetExceededBody(alloc), .json = true };
            },
            error.GraphAnchorFilterRequiresIndex => {
                std.log.warn("public table graph anchor filter lacks native index coverage table={s}", .{table_name});
                return .{ .status = 422, .body = try graphAnchorFilterRequiresIndexBody(alloc), .json = true };
            },
            error.GraphMatchOperationLimitExceeded => {
                std.log.warn("public table graph MATCH operation limit exceeded table={s}", .{table_name});
                return .{ .status = 422, .body = try graphMatchOperationLimitExceededBody(alloc, body), .json = true };
            },
            error.GraphQueryModeUnsupported => {
                std.log.warn("public table graph mode lacks exact public execution table={s}", .{table_name});
                return .{ .status = 422, .body = try graphQueryUnsupportedBody(alloc, body), .json = true };
            },
            error.GraphExternalAliasDocumentFilterUnsupported => {
                std.log.warn("public table graph external alias filter lacks exact runtime support table={s}", .{table_name});
                return .{ .status = 422, .body = try graphQueryCapabilityUnsupportedBody(alloc, body, "external_alias_document_filter_not_supported"), .json = true };
            },
            error.GraphExternalAliasSourceUnsupported => {
                std.log.warn("public table graph external alias source lacks exact runtime support table={s}", .{table_name});
                return .{ .status = 422, .body = try graphQueryCapabilityUnsupportedBody(alloc, body, "external_alias_source_not_supported"), .json = true };
            },
            error.GraphReverseVariablePathUnsupported => {
                std.log.warn("public table graph reverse variable path lacks exact runtime support table={s}", .{table_name});
                return .{ .status = 422, .body = try graphQueryCapabilityUnsupportedBody(alloc, body, "reverse_variable_path_not_supported"), .json = true };
            },
            error.HierarchyCursorStale => {
                std.log.info("public hierarchy traversal cursor stale table={s}", .{table_name});
                return .{ .status = 409, .body = try hierarchyCursorStaleBody(alloc), .json = true };
            },
            error.TopologyChanged => {
                std.log.info("public table query topology changed after retry table={s}", .{table_name});
                return .{
                    .status = 409,
                    .body = try topologyChangedBody(alloc),
                    .json = true,
                };
            },
            error.QueryEmbeddingInputTooLarge => {
                return .{ .status = 413, .body = try alloc.dupe(u8, "query embedding input too large") };
            },
            error.QueryEmbeddingOverloaded => {
                return .{ .status = 429, .body = try alloc.dupe(u8, "query embedding overloaded") };
            },
            error.EmbedRateLimited => {
                return .{ .status = 429, .body = try alloc.dupe(u8, "query embedding rate limited") };
            },
            error.EmbedTransientFailure => {
                std.log.warn("public table query embedding temporarily unavailable table={s}", .{table_name});
                return try queryTemporarilyUnavailableOwnedResponse(alloc, .query_embedding_temporarily_unavailable);
            },
            error.EmbedUpstreamFailure => {
                std.log.warn("public table query embedding upstream failure table={s}", .{table_name});
                return .{ .status = 502, .body = try alloc.dupe(u8, "query embedding provider failed") };
            },
            error.IncompletePublishedSnapshot => {
                std.log.warn("public table query detected incomplete index generation table={s}", .{table_name});
                return try queryTemporarilyUnavailableOwnedResponse(alloc, .index_rebuilding);
            },
            error.InvalidManifest,
            error.InvalidTableFile,
            error.TableBlockChecksumMismatch,
            error.CorruptInput,
            error.UnsupportedVersion,
            error.Corrupted,
            => {
                std.log.err("public table query storage unreadable table={s} err={}", .{ table_name, err });
                return .{
                    .status = 500,
                    .body = try tableStorageUnreadableBody(alloc, err),
                    .json = true,
                };
            },
            error.UnsupportedExactSort => {
                std.log.warn("public table query unsupported exact sort table={s} err={}", .{ table_name, err });
                return .{ .status = 422, .body = try unsupportedExactSortBody(alloc), .json = true };
            },
            error.Canceled => return error.Canceled,
            error.DeadlineExceeded => return error.DeadlineExceeded,
            error.InternalFailure => {
                std.log.err("public table query failed table={s} err={}", .{ table_name, err });
                return .{ .status = 500, .body = try alloc.dupe(u8, "query failed") };
            },
        }
    };
    return .{
        .status = 200,
        .body = response_body,
    };
}

fn publicFilterQueryErrorResponse(
    alloc: std.mem.Allocator,
    body: []const u8,
    field: []const u8,
    kind: query_contract.PublicFilterQueryErrorKind,
) !OwnedResponse {
    return .{
        .status = query_contract.publicFilterQueryErrorStatus(kind),
        .body = try query_contract.encodePublicFilterQueryErrorBodyAlloc(
            alloc,
            body,
            field,
            kind,
        ),
        .json = true,
    };
}

fn bodyHasInternalShardQueryFields(alloc: std.mem.Allocator, body: []const u8) !bool {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidQueryRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    return objectHasInternalShardQueryField(parsed.value.object);
}

fn objectHasInternalShardQueryField(object: std.json.ObjectMap) bool {
    const internal_fields = [_][]const u8{
        "_distributed_text_stats",
        "native_doc_id_constraints",
        "_filter_query_json",
        "_exclusion_query_json",
        "_identity_read_generation",
        "identity_read_generation",
        "_filter_doc_ids",
        "_filter_doc_ids_positive",
        "_exclude_doc_ids",
        "allow_doc_identity_reassignment",
    };
    inline for (internal_fields) |field| {
        if (object.get(field) != null) return true;
    }
    return false;
}

pub fn handleTableQueryView(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    view: TableApi.TableQueryView,
    api: TableApi,
) !OwnedResponse {
    const response_body = api.executeTableQueryView(alloc, table_name, view) catch |err| switch (err) {
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.DocIdentityUnavailable => return try queryTemporarilyUnavailableOwnedResponse(alloc, .doc_identity_unavailable),
        error.ReadRequiresPrimary => return try queryTemporarilyUnavailableOwnedResponse(alloc, .read_requires_primary),
        error.ReadUnavailable => return try queryTemporarilyUnavailableOwnedResponse(alloc, .standby_read_unavailable),
        error.StorageReadTemporarilyUnavailable => return try storageReadTemporarilyUnavailableOwnedResponse(alloc),
        error.ModelNotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "{\"error\":\"MODEL_NOT_FOUND\",\"message\":\"model not found\"}") },
        error.Canceled => return error.Canceled,
        error.DeadlineExceeded => return error.DeadlineExceeded,
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "query failed") },
    };
    return .{
        .status = 200,
        .body = response_body,
    };
}

pub fn handleTableBackup(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    api: TableApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
) !OwnedResponse {
    return handleTableBackupExpectedFence(alloc, table_name, body, null, api, secret_store, node_config, io);
}

pub fn handleTableBackupExpectedFence(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    expected_fence: ?backups_api.TableBackupFence,
    api: TableApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
) !OwnedResponse {
    const parsed_req = backups_api.parseBackupRequest(alloc, body) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") };
    };
    defer parsed_req.deinit();
    backups_api.validateBackupId(parsed_req.value.backup_id) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup id") };
    };

    const backup_format = parseBackupFormat(parsed_req.value.format) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported backup format") };
    };
    var location = backups_api.openBackupLocationWithOptions(alloc, parsed_req.value.location, .{
        .secret_store = secret_store,
        .node_config = node_config,
        .connection = parsed_req.value.connection,
        .required_capability = "backup.write",
        .io = io,
    }) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    api.executeTableBackup(alloc, table_name, parsed_req.value.backup_id, backup_format, expected_fence, parsed_req.value.location, parsed_req.value.connection, &location) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.MetadataCapabilityUnavailable => return .{
            .status = 503,
            .body = try alloc.dupe(u8, backups_api.metadata_capability_unavailable_body),
            .json = true,
            .retry_after_seconds = backups_api.metadata_capability_retry_after_seconds,
        },
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.CatalogChanged => return .{ .status = 409, .body = try alloc.dupe(u8, backups_api.catalog_changed_body), .json = true },
        error.BackupAlreadyExists => return .{ .status = 409, .body = try alloc.dupe(u8, backups_api.backup_already_exists_body), .json = true },
        error.BackupOutcomeAmbiguous => {
            var fallback_io: ?std.Io.Threaded = if (io == null)
                std.Io.Threaded.init(std.heap.page_allocator, .{})
            else
                null;
            defer if (fallback_io) |*owned| owned.deinit();
            const response_io = io orelse fallback_io.?.io();
            const artifact_backup_id = backups_api.tableBackupAttemptArtifactIdAlloc(
                alloc,
                response_io,
                &location,
                parsed_req.value.backup_id,
            ) catch null;
            defer if (artifact_backup_id) |value| alloc.free(value);
            return .{
                .status = 409,
                .body = try backups_api.encodeBackupOutcomeAmbiguousBody(
                    alloc,
                    parsed_req.value.backup_id,
                    artifact_backup_id,
                ),
                .json = true,
            };
        },
        error.BackupManifestTooLarge => return .{ .status = 400, .body = try alloc.dupe(u8, backups_api.manifest_too_large_message) },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.UnsupportedBackupFormat => return .{ .status = 400, .body = try alloc.dupe(u8, "native backup does not support one or more configured index backends") },
        error.UnsupportedBackupMigrationState => return .{
            .status = 400,
            .body = try backups_api.encodeErrorBody(alloc, "backup does not support active schema migration"),
            .json = true,
        },
        error.UnsupportedMultiRangeTable => return .{ .status = 400, .body = try alloc.dupe(u8, "backup does not support multi-range tables") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "backup failed") },
    };

    return .{
        .status = 201,
        .body = try backups_api.encodeBackupSuccess(alloc),
        .json = true,
    };
}

fn parseBackupFormat(value: ?[]const u8) !backups_api.BackupFormat {
    return backups_api.parseBackupFormat(value);
}

pub fn handleTableRestore(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    api: TableApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
) !OwnedResponse {
    const parsed_req = backups_api.parseRestoreRequest(alloc, body) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") };
    };
    defer parsed_req.deinit();
    backups_api.validateBackupId(parsed_req.value.backup_id) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup id") };
    };

    var location = backups_api.openBackupLocationWithOptions(alloc, parsed_req.value.location, .{
        .secret_store = secret_store,
        .node_config = node_config,
        .connection = parsed_req.value.connection,
        .required_capability = "restore.read",
        .io = io,
    }) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    api.executeTableRestore(
        alloc,
        table_name,
        parsed_req.value.backup_id,
        parsed_req.value.location,
        parsed_req.value.connection,
        &location,
    ) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotLeader => return err,
        error.TableAlreadyExists => return .{ .status = 400, .body = try alloc.dupe(u8, "restore target already exists") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.BackupManifestTooLarge => return .{ .status = 400, .body = try alloc.dupe(u8, backups_api.manifest_too_large_message) },
        error.UnsupportedBackupMigrationState => return .{ .status = 400, .body = try alloc.dupe(u8, "restore does not support active schema migration") },
        error.UnsupportedMultiRangeTable => return .{ .status = 400, .body = try alloc.dupe(u8, "restore does not support multi-range tables") },
        error.UnsupportedBackupFormat => return .{ .status = 400, .body = try alloc.dupe(u8, "restore does not support this backup layout") },
        error.RestoreValidationPending => return .{ .status = 503, .body = try alloc.dupe(u8, "restore validation is temporarily unavailable; retry later") },
        error.RestoreDurabilityPending => return .{ .status = 202, .body = try backups_api.encodeRestoreDurabilityPending(alloc), .json = true },
        error.RestoreDurabilityConfirmed => return .{ .status = 200, .body = try backups_api.encodeRestoreDurabilityConfirmed(alloc), .json = true },
        error.BackupIntegrityFailure => return .{ .status = 422, .body = try alloc.dupe(u8, backups_api.integrity_failure_message) },
        error.RestoreDestinationReauthorizationRequired => return .{
            .status = 409,
            .body = try alloc.dupe(u8, "restore was queued before destination authorization was recorded; resubmit it to reauthorize CDC and graph destinations"),
        },
        error.UnsupportedArtifactIndexSources => return .{
            .status = 400,
            .body = try alloc.dupe(u8, "{\"error\":\"unsupported_index_capability\",\"message\":\"artifact-backed index sources are not supported by this deployment\",\"retryable\":false}"),
            .json = true,
        },
        error.ArtifactIndexSourcesTemporarilyUnavailable => return .{
            .status = 503,
            .body = try alloc.dupe(u8, "{\"error\":\"index_capability_upgrade_pending\",\"message\":\"artifact-backed index sources are temporarily unavailable until every live table-serving store supports them\",\"retryable\":true}"),
            .json = true,
            .retry_after_seconds = 1,
        },
        error.InvalidBackupRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "restore failed") },
    };

    return .{
        .status = 202,
        .body = try backups_api.encodeRestoreTriggered(alloc),
        .json = true,
    };
}

test "public table backup and restore require named connections" {
    var backup = try handleTableBackup(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"s3://archive/snap\"}",
        undefined,
        null,
        null,
        null,
    );
    defer backup.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), backup.status);
    try std.testing.expectEqualStrings("invalid backup request", backup.body);

    var restore = try handleTableRestore(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"s3://archive/snap\"}",
        undefined,
        null,
        null,
        null,
    );
    defer restore.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore.status);
    try std.testing.expectEqualStrings("invalid restore request", restore.body);
}

fn testBackupNodeConfig(alloc: std.mem.Allocator) !common_config.Config {
    return common_config.Config.parseFromSlice(alloc,
        \\{
        \\  "connections": {
        \\    "test-backups": {
        \\      "kind": "external_io",
        \\      "capabilities": ["backup.write", "restore.read"],
        \\      "external_io": { "protocol": "filesystem", "root": "/" }
        \\    }
        \\  }
        \\}
    );
}

pub fn handleTableListIndexes(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    const response_body = api.executeTableListIndexes(alloc, table_name) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index list failed") },
    };
    return .{ .status = 200, .body = response_body, .json = true };
}

pub fn handleTableGetIndex(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    const response_body = api.executeTableGetIndex(alloc, table_name, index_name) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index lookup failed") },
    };
    return .{ .status = 200, .body = response_body, .json = true };
}

pub fn handleTableCreateIndex(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    body: []const u8,
    api: TableApi,
) !OwnedResponse {
    const response_body = api.executeTableCreateIndex(alloc, table_name, index_name, body) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "{\"error\":\"not_found\",\"message\":\"not found\",\"retryable\":false}"), .json = true },
        error.Conflict => return .{ .status = 409, .body = try alloc.dupe(u8, "{\"error\":\"table_mutation_conflict\",\"message\":\"table mutation conflict; retry request\",\"retryable\":true}"), .json = true },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "{\"error\":\"method_not_allowed\",\"message\":\"method not allowed\",\"retryable\":false}"), .json = true },
        error.InvalidIndexRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "{\"error\":\"invalid_index_request\",\"message\":\"unsupported index configuration\",\"retryable\":false}"), .json = true },
        error.UnsupportedArtifactIndexSources => return .{ .status = 400, .body = try alloc.dupe(u8, "{\"error\":\"unsupported_index_capability\",\"message\":\"artifact-backed index sources are not supported by this deployment\",\"retryable\":false}"), .json = true },
        error.ArtifactIndexSourcesTemporarilyUnavailable => return .{
            .status = 503,
            .body = try alloc.dupe(u8, "{\"error\":\"index_capability_upgrade_pending\",\"message\":\"artifact-backed index sources are temporarily unavailable until every live table-serving store supports them\",\"retryable\":true}"),
            .json = true,
            .retry_after_seconds = 1,
        },
        error.ProbeUnavailable => return .{
            .status = 503,
            .body = try alloc.dupe(u8, "{\"error\":\"index_probe_unavailable\",\"message\":\"index validation probe unavailable\",\"retryable\":true}"),
            .json = true,
            .retry_after_seconds = 1,
        },
        error.ModelNotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "{\"error\":\"model_not_found\",\"message\":\"model not found\",\"retryable\":false}"), .json = true },
        error.Backpressured => return .{
            .status = 429,
            .body = try alloc.dupe(u8, "{\"code\":\"storage_resource_exhausted\",\"error\":\"storage_resource_exhausted\",\"message\":\"storage descriptors are temporarily exhausted\",\"retryable\":true,\"retry_after_ms\":1000}"),
            .json = true,
            .retry_after_seconds = 1,
        },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "{\"error\":\"internal_error\",\"message\":\"index create failed\",\"retryable\":false}"), .json = true },
    };
    return .{ .status = 201, .body = response_body, .json = true };
}

pub fn handleTableDeleteIndex(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    api.executeTableDeleteIndex(alloc, table_name, index_name) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.Conflict => return .{ .status = 409, .body = try alloc.dupe(u8, "table mutation conflict; retry request") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index delete failed") },
    };
    return .{ .status = 201, .body = try alloc.dupe(u8, "{}"), .json = true };
}

pub fn handlePutArtifactEnrichment(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    artifact_name: []const u8,
    body: []const u8,
    api: TableApi,
) !OwnedResponse {
    api.executePutArtifactEnrichment(alloc, table_name, artifact_name, body) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.Conflict => return .{ .status = 409, .body = try alloc.dupe(u8, "table mutation conflict; retry request") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InvalidEnrichmentRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported artifact enrichment configuration") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact enrichment update failed") },
    };
    return .{ .status = 201, .body = try alloc.dupe(u8, "{}") };
}

pub fn handleDeleteArtifactEnrichment(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    artifact_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    api.executeDeleteArtifactEnrichment(alloc, table_name, artifact_name) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.Conflict => return .{ .status = 409, .body = try alloc.dupe(u8, "table mutation conflict; retry request") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InvalidEnrichmentRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported artifact enrichment configuration") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact enrichment delete failed") },
    };
    return .{ .status = 201, .body = try alloc.dupe(u8, "{}") };
}

pub fn handleListArtifactEnrichments(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    const response_body = api.executeListArtifactEnrichments(alloc, table_name) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact enrichment list failed") },
    };
    return .{ .status = 200, .body = response_body, .json = true };
}

pub fn handleDocumentArtifactManifest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    opts: DocumentArtifactManifestOptions,
    api: TableApi,
) !OwnedResponse {
    var manifest = api.executeDocumentArtifactManifest(alloc, table_name, doc_key, artifact_name) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.ReadRequiresPrimary => return .{ .status = 503, .body = try alloc.dupe(u8, "read requires primary") },
        error.ReadUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "standby read unavailable") },
        error.StorageReadTemporarilyUnavailable => return try storageReadTemporarilyUnavailableOwnedResponse(alloc),
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact manifest lookup failed") },
    };
    defer manifest.deinit(alloc);

    const ChildRangeResponse = struct {
        range_id: []const u8,
        range_kind: []const u8,
        artifact_name: []const u8,
        split_boundary: []const u8,
        placement: []const u8,
        owner_group_id: ?u64,
        placement_generation: ?u64,
        route_status: ?[]const u8,
        split_eligible: ?bool,
        start_key: []const u8,
        end_key_exclusive: []const u8,
        last_key: []const u8,
        child_count: usize,
        text_bytes: ?usize,
    };
    const Response = struct {
        document_id: []const u8,
        artifact_name: []const u8,
        artifact_id: []const u8,
        manifest_version: u64,
        generation: u64,
        source_url: []const u8,
        source_fingerprint: []const u8,
        content_type: []const u8,
        route_type: []const u8,
        unsupported_reason: ?[]const u8,
        unit_count: usize,
        chunk_count: usize,
        ocr_attempted_count: usize,
        ocr_selected_count: usize,
        ocr_retained_embedded_count: usize,
        ocr_failed_count: usize,
        ocr_failed_page_numbers: []const i64,
        ocr_failed_pages_truncated: bool,
        child_ranges: []const ChildRangeResponse,
        child_range_count: usize,
        merge_status: []const u8,
        merge_from_generation: u64,
        merge_to_generation: u64,
        merge_operation_granularity: []const u8,
        merge_operation_count: usize,
        last_error_code: ?[]const u8,
        last_error_message: ?[]const u8,
        manifest_json: ?[]const u8 = null,
        state_json: ?[]const u8,
    };
    const child_ranges = try alloc.alloc(ChildRangeResponse, manifest.child_ranges.len);
    defer alloc.free(child_ranges);
    for (manifest.child_ranges, child_ranges) |child_range, *out| {
        out.* = .{
            .range_id = child_range.range_id,
            .range_kind = child_range.range_kind,
            .artifact_name = child_range.artifact_name,
            .split_boundary = child_range.split_boundary,
            .placement = child_range.placement,
            .owner_group_id = child_range.owner_group_id,
            .placement_generation = child_range.placement_generation,
            .route_status = child_range.route_status,
            .split_eligible = child_range.split_eligible,
            .start_key = child_range.start_key,
            .end_key_exclusive = child_range.end_key_exclusive,
            .last_key = child_range.last_key,
            .child_count = child_range.child_count,
            .text_bytes = child_range.text_bytes,
        };
    }
    return .{
        .status = 200,
        .body = try std.json.Stringify.valueAlloc(alloc, Response{
            .document_id = manifest.document_id,
            .artifact_name = manifest.artifact_name,
            .artifact_id = manifest.artifact_id,
            .manifest_version = manifest.manifest_version,
            .generation = manifest.generation,
            .source_url = manifest.source_url,
            .source_fingerprint = manifest.source_fingerprint,
            .content_type = manifest.content_type,
            .route_type = manifest.route_type,
            .unsupported_reason = manifest.unsupported_reason,
            .unit_count = manifest.unit_count,
            .chunk_count = manifest.chunk_count,
            .ocr_attempted_count = manifest.ocr_attempted_count,
            .ocr_selected_count = manifest.ocr_selected_count,
            .ocr_retained_embedded_count = manifest.ocr_retained_embedded_count,
            .ocr_failed_count = manifest.ocr_failed_count,
            .ocr_failed_page_numbers = manifest.ocr_failed_page_numbers,
            .ocr_failed_pages_truncated = manifest.ocr_failed_pages_truncated,
            .child_ranges = child_ranges,
            .child_range_count = manifest.child_range_count,
            .merge_status = manifest.merge_status,
            .merge_from_generation = manifest.merge_from_generation,
            .merge_to_generation = manifest.merge_to_generation,
            .merge_operation_granularity = manifest.merge_operation_granularity,
            .merge_operation_count = manifest.merge_operation_count,
            .last_error_code = manifest.last_error_code,
            .last_error_message = manifest.last_error_message,
            .manifest_json = if (opts.detail == .raw) manifest.manifest_json else null,
            .state_json = if (opts.detail == .raw) manifest.state_json else null,
        }, .{ .emit_null_optional_fields = false }),
    };
}

pub fn handleDocumentArtifactManifests(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    doc_key: []const u8,
    opts: DocumentArtifactManifestOptions,
    api: TableApi,
) !OwnedResponse {
    var list = api.executeDocumentArtifactManifests(alloc, table_name, doc_key) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.ReadRequiresPrimary => return .{ .status = 503, .body = try alloc.dupe(u8, "read requires primary") },
        error.ReadUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "standby read unavailable") },
        error.StorageReadTemporarilyUnavailable => return try storageReadTemporarilyUnavailableOwnedResponse(alloc),
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact manifest list failed") },
    };
    defer list.deinit(alloc);

    const ChildRangeResponse = struct {
        range_id: []const u8,
        range_kind: []const u8,
        artifact_name: []const u8,
        split_boundary: []const u8,
        placement: []const u8,
        owner_group_id: ?u64,
        placement_generation: ?u64,
        route_status: ?[]const u8,
        split_eligible: ?bool,
        start_key: []const u8,
        end_key_exclusive: []const u8,
        last_key: []const u8,
        child_count: usize,
        text_bytes: ?usize,
    };
    const ManifestResponse = struct {
        document_id: []const u8,
        artifact_name: []const u8,
        artifact_id: []const u8,
        manifest_version: u64,
        generation: u64,
        source_url: []const u8,
        source_fingerprint: []const u8,
        content_type: []const u8,
        route_type: []const u8,
        unsupported_reason: ?[]const u8,
        unit_count: usize,
        chunk_count: usize,
        ocr_attempted_count: usize,
        ocr_selected_count: usize,
        ocr_retained_embedded_count: usize,
        ocr_failed_count: usize,
        ocr_failed_page_numbers: []const i64,
        ocr_failed_pages_truncated: bool,
        child_ranges: []const ChildRangeResponse,
        child_range_count: usize,
        merge_status: []const u8,
        merge_from_generation: u64,
        merge_to_generation: u64,
        merge_operation_granularity: []const u8,
        merge_operation_count: usize,
        last_error_code: ?[]const u8,
        last_error_message: ?[]const u8,
        manifest_json: ?[]const u8 = null,
        state_json: ?[]const u8,
    };
    const Response = struct {
        document_id: []const u8,
        artifacts: []const ManifestResponse,
    };

    const artifacts = try alloc.alloc(ManifestResponse, list.artifacts.len);
    defer alloc.free(artifacts);
    var child_range_sets = std.ArrayListUnmanaged([]ChildRangeResponse).empty;
    defer {
        for (child_range_sets.items) |child_ranges| alloc.free(child_ranges);
        child_range_sets.deinit(alloc);
    }
    for (list.artifacts, artifacts) |manifest, *out| {
        const child_ranges = try alloc.alloc(ChildRangeResponse, manifest.child_ranges.len);
        errdefer alloc.free(child_ranges);
        for (manifest.child_ranges, child_ranges) |child_range, *child_out| {
            child_out.* = .{
                .range_id = child_range.range_id,
                .range_kind = child_range.range_kind,
                .artifact_name = child_range.artifact_name,
                .split_boundary = child_range.split_boundary,
                .placement = child_range.placement,
                .owner_group_id = child_range.owner_group_id,
                .placement_generation = child_range.placement_generation,
                .route_status = child_range.route_status,
                .split_eligible = child_range.split_eligible,
                .start_key = child_range.start_key,
                .end_key_exclusive = child_range.end_key_exclusive,
                .last_key = child_range.last_key,
                .child_count = child_range.child_count,
                .text_bytes = child_range.text_bytes,
            };
        }
        try child_range_sets.append(alloc, child_ranges);
        out.* = .{
            .document_id = manifest.document_id,
            .artifact_name = manifest.artifact_name,
            .artifact_id = manifest.artifact_id,
            .manifest_version = manifest.manifest_version,
            .generation = manifest.generation,
            .source_url = manifest.source_url,
            .source_fingerprint = manifest.source_fingerprint,
            .content_type = manifest.content_type,
            .route_type = manifest.route_type,
            .unsupported_reason = manifest.unsupported_reason,
            .unit_count = manifest.unit_count,
            .chunk_count = manifest.chunk_count,
            .ocr_attempted_count = manifest.ocr_attempted_count,
            .ocr_selected_count = manifest.ocr_selected_count,
            .ocr_retained_embedded_count = manifest.ocr_retained_embedded_count,
            .ocr_failed_count = manifest.ocr_failed_count,
            .ocr_failed_page_numbers = manifest.ocr_failed_page_numbers,
            .ocr_failed_pages_truncated = manifest.ocr_failed_pages_truncated,
            .child_ranges = child_ranges,
            .child_range_count = manifest.child_range_count,
            .merge_status = manifest.merge_status,
            .merge_from_generation = manifest.merge_from_generation,
            .merge_to_generation = manifest.merge_to_generation,
            .merge_operation_granularity = manifest.merge_operation_granularity,
            .merge_operation_count = manifest.merge_operation_count,
            .last_error_code = manifest.last_error_code,
            .last_error_message = manifest.last_error_message,
            .manifest_json = if (opts.detail == .raw) manifest.manifest_json else null,
            .state_json = if (opts.detail == .raw) manifest.state_json else null,
        };
    }

    return .{
        .status = 200,
        .body = try std.json.Stringify.valueAlloc(alloc, Response{
            .document_id = list.document_id,
            .artifacts = artifacts,
        }, .{ .emit_null_optional_fields = false }),
    };
}

pub fn handleReprocessDocumentArtifact(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    api.executeReprocessDocumentArtifact(alloc, table_name, doc_key, artifact_name) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact reprocess failed") },
    };
    return .{
        .status = 202,
        .body = try alloc.dupe(u8, "{\"reprocess\":\"triggered\"}"),
    };
}

pub fn handleReprocessDocumentArtifactRange(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    artifact_name: []const u8,
    body: []const u8,
    api: TableApi,
) !OwnedResponse {
    const Request = struct {
        from_key: []const u8 = "",
        to_key: []const u8 = "",
        limit: u32 = 100,
        shard_cursors: []const db_mod.types.DocumentArtifactReprocessShardResume = &.{},
    };
    const FailureResponse = struct {
        key: []const u8,
        error_code: []const u8,
    };
    const ShardCursorResponse = struct {
        group_id: ?u64,
        next_key: []const u8,
        scanned: usize,
        reprocessed: usize,
        skipped: usize,
        failed: usize,
        limit: u32,
    };
    const Response = struct {
        reprocess: []const u8,
        reprocess_status: []const u8,
        artifact_name: []const u8,
        scanned: usize,
        reprocessed: usize,
        skipped: usize,
        failed: usize,
        limit: u32,
        next_key: ?[]const u8,
        pending_shards: usize,
        failures: []const FailureResponse,
        shard_cursors: []const ShardCursorResponse,
    };

    var parsed = std.json.parseFromSlice(Request, alloc, if (body.len > 0) body else "{}", .{}) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid request") };
    };
    defer parsed.deinit();

    var result = api.executeReprocessDocumentArtifactRange(alloc, table_name, artifact_name, .{
        .from_key = parsed.value.from_key,
        .to_key = parsed.value.to_key,
        .limit = parsed.value.limit,
        .shard_cursors = parsed.value.shard_cursors,
    }) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InvalidRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid request") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact reprocess failed") },
    };
    defer result.deinit(alloc);

    const failures = try alloc.alloc(FailureResponse, result.failures.len);
    defer alloc.free(failures);
    for (result.failures, failures) |failure, *out| {
        out.* = .{
            .key = failure.key,
            .error_code = failure.error_code,
        };
    }
    const shard_cursors = try alloc.alloc(ShardCursorResponse, result.shard_cursors.len);
    defer alloc.free(shard_cursors);
    for (result.shard_cursors, shard_cursors) |cursor, *out| {
        out.* = .{
            .group_id = cursor.group_id,
            .next_key = cursor.next_key,
            .scanned = cursor.scanned,
            .reprocessed = cursor.reprocessed,
            .skipped = cursor.skipped,
            .failed = cursor.failed,
            .limit = cursor.limit,
        };
    }
    const pending_shards = if (result.shard_cursors.len > 0)
        result.shard_cursors.len
    else if (result.next_key != null)
        @as(usize, 1)
    else
        @as(usize, 0);

    return .{
        .status = 202,
        .body = try std.json.Stringify.valueAlloc(alloc, Response{
            .reprocess = "triggered",
            .reprocess_status = if (pending_shards == 0) "complete" else "in_progress",
            .artifact_name = artifact_name,
            .scanned = result.scanned,
            .reprocessed = result.reprocessed,
            .skipped = result.skipped,
            .failed = result.failed,
            .limit = result.limit,
            .next_key = result.next_key,
            .pending_shards = pending_shards,
            .failures = failures,
            .shard_cursors = shard_cursors,
        }, .{}),
    };
}

fn unsupportedBatch(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: db_mod.types.BatchRequest,
    _: operation.RequestContext,
) TableApi.ExecuteBatchError!void {
    return error.InternalFailure;
}

fn unsupportedQueryRequest(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: ?[]const u8,
    _: operation.RequestContext,
) TableApi.ExecuteQueryError![]u8 {
    return error.InternalFailure;
}

fn unsupportedQueryView(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: TableApi.TableQueryView,
    _: operation.RequestContext,
) TableApi.ExecuteQueryViewError![]u8 {
    return error.InternalFailure;
}

fn unsupportedBackup(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: backups_api.BackupFormat,
    _: ?backups_api.TableBackupFence,
    _: []const u8,
    _: []const u8,
    _: *backups_api.BackupLocation,
    _: operation.RequestContext,
) TableApi.ExecuteBackupError!void {
    return error.InternalFailure;
}

fn unsupportedListIndexes(
    _: *anyopaque,
    alloc: std.mem.Allocator,
    _: []const u8,
    _: operation.RequestContext,
) TableApi.ExecuteListIndexesError![]u8 {
    _ = alloc;
    return error.InternalFailure;
}

fn unsupportedGetIndex(
    _: *anyopaque,
    alloc: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: operation.RequestContext,
) TableApi.ExecuteGetIndexError![]u8 {
    _ = alloc;
    return error.InternalFailure;
}

fn unsupportedCreateIndex(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: []const u8,
    _: operation.RequestContext,
) TableApi.ExecuteCreateIndexError![]u8 {
    return error.InternalFailure;
}

fn unsupportedDeleteIndex(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: operation.RequestContext,
) TableApi.ExecuteDeleteIndexError!void {
    return error.InternalFailure;
}

fn unsupportedRestore(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: []const u8,
    _: []const u8,
    _: *backups_api.BackupLocation,
    _: operation.RequestContext,
) TableApi.ExecuteRestoreError!void {
    return error.InternalFailure;
}

test "public table batch handler returns created batch response" {
    const Backend = struct {
        called: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.called = true;
            if (!std.mem.eql(u8, table_name, "docs")) return error.InternalFailure;
            if (req.writes.len != 1) return error.InternalFailure;
        }
    };

    var backend = Backend{};
    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , backend.iface());
    defer resp.deinit(std.testing.allocator);
    var parsed = try std.json.parseFromSlice(batch_api.BatchResult, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expect(backend.called);
    try std.testing.expectEqual(@as(u32, 1), parsed.value.inserted);
}

test "public table api carries borrowed cancellation into batch execution" {
    const Backend = struct {
        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            req: db_mod.types.BatchRequest,
            request: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            _ = req;
            const token = request.cancellation;
            if (!token.isCancelled()) return error.InternalFailure;
            return error.Canceled;
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            request: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            _ = alloc;
            const token = request.cancellation;
            if (!token.isCancelled()) return error.InternalFailure;
            return error.Canceled;
        }

        fn executeTableQueryView(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: TableApi.TableQueryView,
            request: operation.RequestContext,
        ) TableApi.ExecuteQueryViewError![]u8 {
            _ = alloc;
            const token = request.cancellation;
            if (!token.isCancelled()) return error.InternalFailure;
            return error.Canceled;
        }
    };

    var signal = std.atomic.Value(bool).init(true);
    var state: u8 = 0;
    const api = TableApi{
        .ptr = &state,
        .request = .{ .cancellation = db_mod.types.CancellationToken.fromAtomic(&signal) },
        .vtable = &.{
            .execute_table_batch = Backend.executeTableBatch,
            .execute_table_query_request = Backend.executeTableQueryRequest,
            .execute_table_query_view = Backend.executeTableQueryView,
            .execute_table_backup = unsupportedBackup,
            .execute_table_restore = unsupportedRestore,
            .execute_table_list_indexes = unsupportedListIndexes,
            .execute_table_get_index = unsupportedGetIndex,
            .execute_table_create_index = unsupportedCreateIndex,
            .execute_table_delete_index = unsupportedDeleteIndex,
        },
    };

    try std.testing.expectError(error.Canceled, api.executeTableBatch(std.testing.allocator, "docs", .{}));
    try std.testing.expectError(error.Canceled, api.executeTableQueryRequest(std.testing.allocator, "docs", "{}", null));
    try std.testing.expectError(error.Canceled, api.executeTableQueryView(std.testing.allocator, "docs", .published));
}

test "public create index exposes retryable storage descriptor exhaustion" {
    const Backend = struct {
        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = executeCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {}

        fn executeCreateIndex(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteCreateIndexError![]u8 {
            return error.Backpressured;
        }
    };

    var backend = Backend{};
    var resp = try handleTableCreateIndex(std.testing.allocator, "docs", "search", "{}", backend.iface());
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 429), resp.status);
    try std.testing.expect(resp.json);
    try std.testing.expectEqual(@as(?u32, 1), resp.retry_after_seconds);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "storage_resource_exhausted") != null);
}

test "public create index exposes unsupported deployment capability" {
    const Backend = struct {
        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = executeCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {}

        fn executeCreateIndex(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteCreateIndexError![]u8 {
            return error.UnsupportedArtifactIndexSources;
        }
    };

    var backend = Backend{};
    var resp = try handleTableCreateIndex(std.testing.allocator, "docs", "search", "{}", backend.iface());
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expect(resp.json);
    try std.testing.expectEqualStrings(
        "{\"error\":\"unsupported_index_capability\",\"message\":\"artifact-backed index sources are not supported by this deployment\",\"retryable\":false}",
        resp.body,
    );
}

test "public create index returns normalized created resource" {
    const Backend = struct {
        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = executeCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {}

        fn executeCreateIndex(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteCreateIndexError![]u8 {
            return alloc.dupe(u8, "{\"name\":\"search\",\"type\":\"full_text\"}") catch error.InternalFailure;
        }
    };

    var backend = Backend{};
    var resp = try handleTableCreateIndex(std.testing.allocator, "docs", "search", "{\"type\":\"full_text\"}", backend.iface());
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expect(resp.json);
    try std.testing.expectEqualStrings("{\"name\":\"search\",\"type\":\"full_text\"}", resp.body);
}

test "public table batch handler forwards pull transforms" {
    const Backend = struct {
        called: bool = false,
        op: ?db_mod.types.TransformOpType = null,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            request: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.called = true;
            if (request.transforms.len != 1 or request.transforms[0].operations.len != 1)
                return error.InternalFailure;
            self.op = request.transforms[0].operations[0].op;
        }
    };

    var backend = Backend{};
    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"transforms":[{"key":"doc:missing","operations":[{"op":"$pull","path":"tags","value":"new"}]}]}
    , backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expect(backend.called);
    try std.testing.expectEqual(db_mod.types.TransformOpType.pull, backend.op.?);
}

test "public table batch handler maps backend errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            return error.Backpressured;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 429), resp.status);
    try std.testing.expectEqualStrings("table backpressured", resp.body);
}

test "public table batch handler returns concise dense repair backpressure" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            return error.DenseRepairBackpressure;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 429), resp.status);
    try std.testing.expect(resp.json);
    try std.testing.expectEqual(@as(?u32, 1), resp.retry_after_seconds);
    var parsed = try std.json.parseFromSlice(struct {
        code: []const u8,
        message: []const u8,
        retryable: bool,
        retry_after_ms: u32,
    }, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings("dense_repair_backpressure", parsed.value.code);
    try std.testing.expect(parsed.value.message.len != 0);
    try std.testing.expect(parsed.value.retryable);
    try std.testing.expectEqual(@as(u32, 1000), parsed.value.retry_after_ms);
}

test "public table batch handler maps unavailable errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            return error.Unavailable;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("maintenance routes unavailable on query-only runtime", resp.body);
}

test "public table batch handler maps write unavailable errors" {
    const Backend = struct {
        err: TableApi.ExecuteBatchError,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.err == error.InternalFailure)
                setLastBatchFailureName(error.InferenceProviderFailure);
            return self.err;
        }
    };

    const cases = [_]struct {
        err: TableApi.ExecuteBatchError,
        status: u16,
        body: []const u8,
        json: bool = false,
    }{
        .{ .err = error.WriteUnavailable, .status = 503, .body = "write unavailable" },
        .{
            .err = error.OutcomeUnknown,
            .status = 500,
            .body = "transaction outcome is unknown; do not retry this stateless batch because it may already have committed; use a transaction session for retryable commits",
        },
        .{
            .err = error.InternalFailure,
            .status = 500,
            .body = "{\"error\":\"InferenceProviderFailure\",\"message\":\"batch failed\"}",
            .json = true,
        },
    };
    for (cases) |tc| {
        var backend = Backend{ .err = tc.err };
        var resp = try handleTableBatch(std.testing.allocator, "docs",
            \\{"inserts":{"doc-a":{"title":"alpha"}}}
        , backend.iface());
        defer resp.deinit(std.testing.allocator);

        try std.testing.expectEqual(tc.status, resp.status);
        try std.testing.expectEqualStrings(tc.body, resp.body);
        try std.testing.expectEqual(tc.json, resp.json);
    }
}

test "public table batch handler returns accepted for durable pending commits" {
    const Backend = struct {
        fn iface() TableApi {
            return .{ .ptr = undefined, .request = .{}, .vtable = &.{
                .execute_table_batch = executeTableBatch,
                .execute_table_query_request = unsupportedQueryRequest,
                .execute_table_query_view = unsupportedQueryView,
                .execute_table_backup = unsupportedBackup,
                .execute_table_restore = unsupportedRestore,
                .execute_table_list_indexes = unsupportedListIndexes,
                .execute_table_get_index = unsupportedGetIndex,
                .execute_table_create_index = unsupportedCreateIndex,
                .execute_table_delete_index = unsupportedDeleteIndex,
            } };
        }

        fn executeTableBatch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest, _: operation.RequestContext) TableApi.ExecuteBatchError!void {
            return error.CommittedPending;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"inserted\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"status\":\"committed_pending\"") != null);
}

test "public table batch handler identifies committed repair-required writes" {
    const Backend = struct {
        fn iface() TableApi {
            return .{ .ptr = undefined, .request = .{}, .vtable = &.{
                .execute_table_batch = executeTableBatch,
                .execute_table_query_request = unsupportedQueryRequest,
                .execute_table_query_view = unsupportedQueryView,
                .execute_table_backup = unsupportedBackup,
                .execute_table_restore = unsupportedRestore,
                .execute_table_list_indexes = unsupportedListIndexes,
                .execute_table_get_index = unsupportedGetIndex,
                .execute_table_create_index = unsupportedCreateIndex,
                .execute_table_delete_index = unsupportedDeleteIndex,
            } };
        }

        fn executeTableBatch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest, _: operation.RequestContext) TableApi.ExecuteBatchError!void {
            return error.CommittedRepairRequired;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"inserted\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"status\":\"committed_repair_required\"") != null);
}

test "public table batch handler preserves ambiguous write outcomes" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            return error.WriteOutcomeUnknown;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try std.testing.expectEqualStrings("write outcome unknown", resp.body);
}

test "public table batch handler exposes pending HA durability without claiming rollback" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            return error.HAWriteDurabilityPending;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings(
        "write committed locally; standby durability acknowledgment pending",
        resp.body,
    );
}

test "public table batch handler maps doc identity unavailable errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            return error.DocIdentityUnavailable;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("doc identity unavailable", resp.body);
}

test "public table batch handler maps HA write gate errors" {
    const Backend = struct {
        err: TableApi.ExecuteBatchError,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = executeTableBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBatch(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: db_mod.types.BatchRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteBatchError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.err;
        }
    };

    const Case = struct {
        err: TableApi.ExecuteBatchError,
        body: []const u8,
    };
    const cases = [_]Case{
        .{ .err = error.HAReadOnlyStandby, .body = "standby is read-only" },
        .{ .err = error.HAPromotedStandbyRequiresPrimaryOpen, .body = "promoted standby requires primary open" },
        .{ .err = error.HAFencedPrimary, .body = "fenced primary rejects writes" },
    };

    for (cases) |tc| {
        var backend = Backend{ .err = tc.err };
        var resp = try handleTableBatch(std.testing.allocator, "docs",
            \\{"inserts":{"doc-a":{"title":"alpha"}}}
        , backend.iface());
        defer resp.deinit(std.testing.allocator);

        try std.testing.expectEqual(@as(u16, 409), resp.status);
        try std.testing.expectEqualStrings(tc.body, resp.body);
    }
}

test "public table query handler maps doc identity unavailable errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            return error.DocIdentityUnavailable;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}}}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try expectQueryTemporarilyUnavailableResponse(
        std.testing.allocator,
        resp,
        "doc_identity_unavailable",
        "doc identity unavailable",
    );
}

test "public table query handler preserves structured filter and hierarchy diagnostics" {
    const Backend = struct {
        err: TableApi.ExecuteQueryError,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.err;
        }
    };
    const Case = struct {
        err: TableApi.ExecuteQueryError,
        field: []const u8,
        status: u16,
    };
    const cases = [_]Case{
        .{ .err = error.InvalidFilterQueryRequest, .field = "filter_query", .status = 400 },
        .{ .err = error.InvalidExclusionQueryRequest, .field = "exclusion_query", .status = 400 },
        .{ .err = error.UnsupportedFilterQueryRequest, .field = "filter_query", .status = 422 },
        .{ .err = error.UnsupportedExclusionQueryRequest, .field = "exclusion_query", .status = 422 },
    };
    const body =
        \\{"filter_query":{"disjuncts":[{"query_string":"status:active"}]},"exclusion_query":{"disjuncts":[{"query_string":"status:deleted"}]}}
    ;
    const Parsed = struct {
        status: u16,
        field: []const u8,
        offending_node: []const u8,
        retryable: bool,
    };

    for (cases) |tc| {
        var backend = Backend{ .err = tc.err };
        var resp = try handleTableQueryRequest(
            std.testing.allocator,
            "docs",
            body,
            null,
            backend.iface(),
        );
        defer resp.deinit(std.testing.allocator);
        try std.testing.expectEqual(tc.status, resp.status);
        try std.testing.expect(resp.json);

        var parsed = try std.json.parseFromSlice(
            Parsed,
            std.testing.allocator,
            resp.body,
            .{ .ignore_unknown_fields = true },
        );
        defer parsed.deinit();
        try std.testing.expectEqual(tc.status, parsed.value.status);
        try std.testing.expectEqualStrings(tc.field, parsed.value.field);
        try std.testing.expectEqualStrings("query_string", parsed.value.offending_node);
        try std.testing.expect(!parsed.value.retryable);
    }

    var hierarchy_backend = Backend{ .err = error.UnsupportedHierarchyGrouping };
    var hierarchy_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{}",
        null,
        hierarchy_backend.iface(),
    );
    defer hierarchy_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), hierarchy_resp.status);
    try std.testing.expect(hierarchy_resp.json);
    var hierarchy_error = try std.json.parseFromSlice(UnsupportedHierarchyGroupingError, std.testing.allocator, hierarchy_resp.body, .{});
    defer hierarchy_error.deinit();
    try std.testing.expectEqualStrings("unsupported_hierarchy_grouping", hierarchy_error.value.@"error");
    try std.testing.expectEqualStrings("unit_identity_unavailable", hierarchy_error.value.reason);
    try std.testing.expectEqualStrings("hierarchy.group_by.level", hierarchy_error.value.field);
    try std.testing.expectEqualStrings("use_source_grouping_or_direct_members", hierarchy_error.value.action);
    try std.testing.expect(std.mem.indexOf(u8, hierarchy_error.value.message, "return_mode") == null);
    try std.testing.expect(!hierarchy_error.value.retryable);

    var unsupported_backend = Backend{ .err = error.UnsupportedQueryRequest };
    var unsupported_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{}",
        null,
        unsupported_backend.iface(),
    );
    defer unsupported_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), unsupported_resp.status);
    try std.testing.expect(unsupported_resp.json);
    var unsupported_error = try std.json.parseFromSlice(UnsupportedQueryError, std.testing.allocator, unsupported_resp.body, .{});
    defer unsupported_error.deinit();
    try std.testing.expectEqualStrings("unsupported_query_request", unsupported_error.value.@"error");
    try std.testing.expectEqualStrings("unsupported query request", unsupported_error.value.message);
    try std.testing.expect(!unsupported_error.value.retryable);
}

test "public table query handler preserves retryable failure status" {
    const Backend = struct {
        err: TableApi.ExecuteQueryError,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.err;
        }
    };
    const Case = struct {
        err: TableApi.ExecuteQueryError,
        status: u16,
        body: []const u8,
        json: bool = false,
        unavailable_code: ?[]const u8 = null,
        unavailable_message: []const u8 = "",
    };
    const cases = [_]Case{
        .{ .err = error.QueryEmbeddingInputTooLarge, .status = 413, .body = "query embedding input too large" },
        .{ .err = error.QueryEmbeddingOverloaded, .status = 429, .body = "query embedding overloaded" },
        .{ .err = error.EmbedRateLimited, .status = 429, .body = "query embedding rate limited" },
        .{ .err = error.EmbedTransientFailure, .status = 503, .body = "", .json = true, .unavailable_code = "query_embedding_temporarily_unavailable", .unavailable_message = "query embedding temporarily unavailable" },
        .{ .err = error.EmbedUpstreamFailure, .status = 502, .body = "query embedding provider failed" },
        .{ .err = error.IndexRebuilding, .status = 503, .body = "", .json = true, .unavailable_code = "index_rebuilding", .unavailable_message = "required index is rebuilding" },
        .{ .err = error.StorageReadTemporarilyUnavailable, .status = 503, .body = "", .json = true, .unavailable_code = "storage_read_temporarily_unavailable", .unavailable_message = "storage read temporarily unavailable" },
        .{ .err = error.HierarchyCursorStale, .status = 409, .body = "{\"status\":409,\"error\":\"hierarchy_cursor_stale\",\"message\":\"the source hierarchy changed after this cursor was issued\",\"action\":\"restart_hierarchy_traversal\",\"restart_without\":\"search_after\",\"retryable\":false}", .json = true },
        .{ .err = error.InvalidManifest, .status = 500, .body = "{\"code\":\"table_storage_unreadable\",\"error\":\"InvalidManifest\",\"message\":\"table storage unreadable\",\"retryable\":false}", .json = true },
        .{ .err = error.CorruptInput, .status = 500, .body = "{\"code\":\"table_storage_unreadable\",\"error\":\"CorruptInput\",\"message\":\"table storage unreadable\",\"retryable\":false}", .json = true },
        .{ .err = error.IncompletePublishedSnapshot, .status = 503, .body = "", .json = true, .unavailable_code = "index_rebuilding", .unavailable_message = "required index is rebuilding" },
    };

    for (cases) |tc| {
        var backend = Backend{ .err = tc.err };
        var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
            \\{"query":{"match_all":{}}}
        , null, backend.iface());
        defer resp.deinit(std.testing.allocator);
        try std.testing.expectEqual(tc.status, resp.status);
        if (tc.unavailable_code) |code| {
            try expectQueryTemporarilyUnavailableResponse(std.testing.allocator, resp, code, tc.unavailable_message);
        } else {
            try std.testing.expectEqualStrings(tc.body, resp.body);
        }
        try std.testing.expectEqual(tc.json, resp.json);
        try std.testing.expectEqual(
            if (tc.unavailable_code != null) @as(?u32, 1) else null,
            resp.retry_after_seconds,
        );
    }
}

test "public table query handler maps HA read gate errors" {
    const Backend = struct {
        err: TableApi.ExecuteQueryError,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.err;
        }
    };

    var primary_backend = Backend{ .err = error.ReadRequiresPrimary };
    var primary_resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}}}
    , null, primary_backend.iface());
    defer primary_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), primary_resp.status);
    try expectQueryTemporarilyUnavailableResponse(
        std.testing.allocator,
        primary_resp,
        "read_requires_primary",
        "read requires primary",
    );

    var lag_backend = Backend{ .err = error.ReadUnavailable };
    var lag_resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}}}
    , null, lag_backend.iface());
    defer lag_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), lag_resp.status);
    try expectQueryTemporarilyUnavailableResponse(
        std.testing.allocator,
        lag_resp,
        "standby_read_unavailable",
        "standby read unavailable",
    );
}

test "public table query handler returns json response" {
    const Backend = struct {
        called: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            body: []const u8,
            row_filter_json: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.called = true;
            if (!std.mem.eql(u8, table_name, "docs")) return error.InternalFailure;
            var parsed = std.json.parseFromSlice(metadata_openapi.QueryRequest, alloc, body, .{ .ignore_unknown_fields = true }) catch return error.InternalFailure;
            defer parsed.deinit();
            if (parsed.value.full_text_search == null) return error.InternalFailure;
            if (row_filter_json == null or !std.mem.eql(u8, row_filter_json.?, "{\"term\":{\"status\":\"published\"}}")) return error.InternalFailure;
            return alloc.dupe(u8, "{\"responses\":[]}") catch error.InternalFailure;
        }
    };

    var backend = Backend{};
    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"full_text_search":{"query":"alpha"}}
    , "{\"term\":{\"status\":\"published\"}}", backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(backend.called);
    try std.testing.expectEqualStrings("{\"responses\":[]}", resp.body);
}

test "public table query handler rejects only top-level internal fields" {
    const Backend = struct {
        called: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.called = true;
            return alloc.dupe(u8, "{\"responses\":[]}") catch error.InternalFailure;
        }
    };

    var rejected_backend = Backend{};
    var rejected = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"_identity_read_generation":1}
    , null, rejected_backend.iface());
    defer rejected.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), rejected.status);
    try std.testing.expect(!rejected_backend.called);

    var rejected_plain_generation_backend = Backend{};
    var rejected_plain_generation = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"with":{"visible":{"match_all":{}}},"identity_read_generation":1}
    , null, rejected_plain_generation_backend.iface());
    defer rejected_plain_generation.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), rejected_plain_generation.status);
    try std.testing.expect(!rejected_plain_generation_backend.called);

    var rejected_reassignment_backend = Backend{};
    var rejected_reassignment = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"allow_doc_identity_reassignment":true}
    , null, rejected_reassignment_backend.iface());
    defer rejected_reassignment.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), rejected_reassignment.status);
    try std.testing.expect(!rejected_reassignment_backend.called);

    var accepted_backend = Backend{};
    var accepted = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"full_text_search":{"query":"mentions \"_identity_read_generation\" and \"native_doc_id_constraints\""}}
    , null, accepted_backend.iface());
    defer accepted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), accepted.status);
    try std.testing.expect(accepted_backend.called);
    try std.testing.expectEqualStrings("{\"responses\":[]}", accepted.body);
}

test "public table query handler maps backend errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            return error.InvalidQueryRequest;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"bad":true}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("invalid query request", resp.body);
}

test "public table query handler maps invalid exact sort diagnostics" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            db_mod.testing.recordSortRejectionDiagnostic(
                "_score",
                "invalid_sort_tuple",
                "non_numeric_score",
            );
            return error.InvalidQueryRequest;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"full_text_search":{"match":"raft","field":"body"},"order_by":[{"field":"_score","desc":true}]}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 422), resp.status);
    try std.testing.expect(resp.json);
    var parsed = try std.json.parseFromSlice(struct {
        status: u16,
        @"error": []const u8,
        message: []const u8,
        reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
    }, std.testing.allocator, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u16, 422), parsed.value.status);
    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.@"error");
    try std.testing.expectEqualStrings("exact sort is unsupported for this query", parsed.value.message);
    try std.testing.expectEqualStrings("invalid_sort_tuple", parsed.value.reason);
    try std.testing.expectEqualStrings("invalid_sort_tuple", parsed.value.sort_rejection_reason);
    try std.testing.expectEqualStrings("invalid_sort_tuple", parsed.value.sort_rejection_detail);
    try std.testing.expectEqualStrings("_score", parsed.value.sort_rejection_field);
}

test "public table query handler rejects unknown sort tuple properties before dispatch" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            return error.InternalFailure;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"order_by":[{"field":"created_at","descc":true}]}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 422), resp.status);
    try std.testing.expect(resp.json);
    var parsed = try std.json.parseFromSlice(struct {
        status: u16,
        @"error": []const u8,
        reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
    }, std.testing.allocator, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.@"error");
    try std.testing.expectEqualStrings("invalid_sort_tuple", parsed.value.reason);
    try std.testing.expectEqualStrings("invalid_sort_tuple", parsed.value.sort_rejection_reason);
    try std.testing.expectEqualStrings("invalid_sort_tuple", parsed.value.sort_rejection_detail);
    try std.testing.expectEqualStrings("created_at", parsed.value.sort_rejection_field);
}

test "public table query handler maps candidate budget exhaustion" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            db_mod.testing.recordSortRejectionDiagnostic(
                "full_text_index_v0",
                "candidate_budget_exceeded",
                "text_field_sort_candidate_window",
            );
            return error.QueryCandidateBudgetExceeded;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"order_by":[{"field":"created_at"}]}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 422), resp.status);
    try std.testing.expect(resp.json);
    var parsed = try std.json.parseFromSlice(struct {
        status: u16,
        @"error": []const u8,
        message: []const u8,
        reason: []const u8,
        budget_rejection_reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
    }, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u16, 422), parsed.value.status);
    try std.testing.expectEqualStrings("query_candidate_budget_exceeded", parsed.value.@"error");
    try std.testing.expectEqualStrings("query candidate budget exceeded", parsed.value.message);
    try std.testing.expectEqualStrings("candidate_budget_exceeded", parsed.value.reason);
    try std.testing.expectEqualStrings("text_field_sort_candidate_window", parsed.value.budget_rejection_reason);
    try std.testing.expectEqualStrings("candidate_budget_exceeded", parsed.value.sort_rejection_reason);
    try std.testing.expectEqualStrings("candidate_budget_exceeded", parsed.value.sort_rejection_detail);
    try std.testing.expectEqualStrings("full_text_index_v0", parsed.value.sort_rejection_field);
}

test "public table query handler maps exact graph execution failures" {
    const Kind = enum {
        work_budget,
        path_weight_domain,
        distinct_budget,
        anchor_filter,
        match_operation_limit,
        graph_query_mode,
        external_alias_filter,
        external_alias_source,
        reverse_variable_path,
        topology_changed,
    };
    const Backend = struct {
        fn iface(kind: *Kind) TableApi {
            return .{
                .ptr = kind,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            const kind: *Kind = @ptrCast(@alignCast(ptr));
            return switch (kind.*) {
                .work_budget => {
                    graph_work_budget_diagnostic.record("pattern", .{
                        .query_type = .pattern,
                        .index_name = "relationships",
                        .start_nodes = .{ .keys = &.{} },
                        .match_pattern = .{ .nodes = &.{}, .edges = &.{} },
                    }, .{ .dimension = .intermediate_states, .maximum = 100_000 });
                    return error.GraphWorkBudgetExceeded;
                },
                .path_weight_domain => {
                    graph_path_weight_diagnostic.record("strongest", .{
                        .query_type = .neighbors,
                        .index_name = "relationships",
                        .start_nodes = .{ .keys = &.{} },
                        .params = .{ .weight_mode = .max_weight },
                    }, error.GraphMaxWeightDomainViolation);
                    return error.GraphMaxWeightDomainViolation;
                },
                .distinct_budget => {
                    graph_distinct_budget_diagnostic.record("unique_people", .{
                        .dimension = .distinct_identities,
                        .maximum = 512,
                    });
                    return error.GraphDistinctBudgetExceeded;
                },
                .anchor_filter => error.GraphAnchorFilterRequiresIndex,
                .match_operation_limit => error.GraphMatchOperationLimitExceeded,
                .graph_query_mode => error.GraphQueryModeUnsupported,
                .external_alias_filter => {
                    graph_query_diagnostic.record("pattern", "match", .external_alias_document_filter_not_supported);
                    return error.GraphExternalAliasDocumentFilterUnsupported;
                },
                .external_alias_source => {
                    graph_query_diagnostic.record("pattern", "match", .external_alias_source_not_supported);
                    return error.GraphExternalAliasSourceUnsupported;
                },
                .reverse_variable_path => {
                    graph_query_diagnostic.record("pattern", "match", .reverse_variable_path_not_supported);
                    return error.GraphReverseVariablePathUnsupported;
                },
                .topology_changed => error.TopologyChanged,
            };
        }
    };

    var kind = Kind.work_budget;
    var work_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{\"query\":{\"match_all\":{}}}",
        null,
        Backend.iface(&kind),
    );
    defer work_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), work_resp.status);
    var work = try ant_json.parseFromSlice(
        metadata_openapi.QueryUnprocessableError,
        std.testing.allocator,
        work_resp.body,
        .{},
    );
    defer work.deinit();
    const work_error = switch (work.value) {
        .graph_work_budget_exceeded_error => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(!work_error.retryable);
    try std.testing.expectEqualStrings("pattern", work_error.operation);
    try std.testing.expectEqualStrings("match", work_error.mode);
    try std.testing.expectEqualStrings("intermediate_states", work_error.dimension);
    try std.testing.expectEqual(@as(i64, 100_000), work_error.maximum);

    kind = .path_weight_domain;
    var weight_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{\"query\":{\"match_all\":{}}}",
        null,
        Backend.iface(&kind),
    );
    defer weight_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), weight_resp.status);
    var weight = try ant_json.parseFromSlice(
        metadata_openapi.QueryUnprocessableError,
        std.testing.allocator,
        weight_resp.body,
        .{},
    );
    defer weight.deinit();
    const weight_error = switch (weight.value) {
        .graph_path_weight_domain_error => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(!weight_error.retryable);
    try std.testing.expectEqualStrings("strongest", weight_error.operation);
    try std.testing.expectEqual(.max_weight_product, weight_error.objective);

    kind = .distinct_budget;
    var resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{\"query\":{\"match_all\":{}}}",
        null,
        Backend.iface(&kind),
    );
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), resp.status);
    var distinct = try ant_json.parseFromSlice(
        metadata_openapi.QueryUnprocessableError,
        std.testing.allocator,
        resp.body,
        .{},
    );
    defer distinct.deinit();
    const distinct_error = switch (distinct.value) {
        .graph_distinct_budget_exceeded_error => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(!distinct_error.retryable);
    try std.testing.expectEqualStrings("unique_people", distinct_error.operation);
    try std.testing.expectEqualStrings("distinct_identities", distinct_error.dimension);
    try std.testing.expectEqual(@as(i64, 512), distinct_error.maximum);
    try std.testing.expect(distinct_error.remediation.len > 0);

    kind = .anchor_filter;
    var anchor_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{\"query\":{\"match_all\":{}}}",
        null,
        Backend.iface(&kind),
    );
    defer anchor_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), anchor_resp.status);
    var anchor = try ant_json.parseFromSlice(
        metadata_openapi.QueryUnprocessableError,
        std.testing.allocator,
        anchor_resp.body,
        .{},
    );
    defer anchor.deinit();
    const anchor_error = switch (anchor.value) {
        .graph_anchor_filter_requires_index_error => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(!anchor_error.retryable);

    kind = .match_operation_limit;
    var match_body = std.ArrayListUnmanaged(u8).empty;
    defer match_body.deinit(std.testing.allocator);
    try match_body.appendSlice(std.testing.allocator, "{\"graph_queries\":{");
    for (0..graph_query_mod.max_match_queries_per_request + 1) |i| {
        if (i > 0) try match_body.append(std.testing.allocator, ',');
        try match_body.print(std.testing.allocator, "\"q{d}\":{{\"match\":{{}}}}", .{i});
    }
    try match_body.appendSlice(std.testing.allocator, "}}");
    var limit_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        match_body.items,
        null,
        Backend.iface(&kind),
    );
    defer limit_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), limit_resp.status);
    var limit = try ant_json.parseFromSlice(
        metadata_openapi.QueryUnprocessableError,
        std.testing.allocator,
        limit_resp.body,
        .{},
    );
    defer limit.deinit();
    const limit_error = switch (limit.value) {
        .graph_match_operation_limit_exceeded_error => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(!limit_error.retryable);
    try std.testing.expectEqual(@as(i64, graph_query_mod.max_match_queries_per_request), limit_error.maximum);
    try std.testing.expectEqual(@as(i64, graph_query_mod.max_match_queries_per_request + 1), limit_error.actual);

    kind = .graph_query_mode;
    var cross_range_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{\"graph_searches\":{\"incoming\":{\"type\":\"traverse\",\"index_name\":\"relationships\",\"start_nodes\":{\"keys\":[\"doc:a\"]},\"params\":{\"direction\":\"in\"}}}}",
        null,
        Backend.iface(&kind),
    );
    defer cross_range_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 422), cross_range_resp.status);
    var graph_mode = try ant_json.parseFromSlice(
        metadata_openapi.QueryUnprocessableError,
        std.testing.allocator,
        cross_range_resp.body,
        .{},
    );
    defer graph_mode.deinit();
    const graph_mode_error = switch (graph_mode.value) {
        .graph_query_unsupported_error => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expect(!graph_mode_error.retryable);
    try std.testing.expectEqualStrings("incoming", graph_mode_error.operation);
    try std.testing.expectEqualStrings("traverse", graph_mode_error.feature);
    try std.testing.expectEqualStrings("direction_must_be_out", graph_mode_error.reason);

    const CapabilityCase = struct { kind: Kind, reason: []const u8 };
    const capability_cases = [_]CapabilityCase{
        .{ .kind = .external_alias_filter, .reason = "external_alias_document_filter_not_supported" },
        .{ .kind = .external_alias_source, .reason = "external_alias_source_not_supported" },
        .{ .kind = .reverse_variable_path, .reason = "reverse_variable_path_not_supported" },
    };
    for (capability_cases) |case| {
        kind = case.kind;
        var capability_resp = try handleTableQueryRequest(
            std.testing.allocator,
            "docs",
            "{\"graph_queries\":{\"first_walk\":{\"index\":\"relationships\",\"traverse\":{\"start\":{\"keys\":[\"doc:a\"]}}},\"pattern\":{\"index\":\"relationships\",\"match\":{\"anchor\":\"a\",\"nodes\":{\"a\":{}},\"edges\":[]},\"return\":{\"bindings\":[\"a\"]}}}}",
            null,
            Backend.iface(&kind),
        );
        defer capability_resp.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u16, 422), capability_resp.status);
        var capability = try ant_json.parseFromSlice(
            metadata_openapi.QueryUnprocessableError,
            std.testing.allocator,
            capability_resp.body,
            .{},
        );
        defer capability.deinit();
        const capability_error = switch (capability.value) {
            .graph_query_unsupported_error => |value| value,
            else => return error.TestUnexpectedResult,
        };
        try std.testing.expectEqualStrings(case.reason, capability_error.reason);
        try std.testing.expectEqualStrings("pattern", capability_error.operation);
        try std.testing.expectEqualStrings("match", capability_error.feature);
    }

    kind = .topology_changed;
    var topology_resp = try handleTableQueryRequest(
        std.testing.allocator,
        "docs",
        "{\"graph_queries\":{\"neighbors\":{\"index\":\"relationships\",\"traverse\":{\"start\":{\"keys\":[\"doc:a\"]}}}}}",
        null,
        Backend.iface(&kind),
    );
    defer topology_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), topology_resp.status);
    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        "{\"status\":409,\"error\":\"topology_changed\",\"action\":\"retry_query\",\"retryable\":true}",
        topology_resp.body,
    );

    const legacy_limit_body =
        \\{"graph_searches":{"first":{"type":"pattern"},"neighbors":{"type":"neighbors"},"second":{"type":"pattern"}}}
    ;
    const legacy_error_body = try graphMatchOperationLimitExceededBody(std.testing.allocator, legacy_limit_body);
    defer std.testing.allocator.free(legacy_error_body);
    var legacy_limit = try ant_json.parseFromSlice(
        metadata_openapi.QueryUnprocessableError,
        std.testing.allocator,
        legacy_error_body,
        .{},
    );
    defer legacy_limit.deinit();
    const legacy_limit_error = switch (legacy_limit.value) {
        .graph_match_operation_limit_exceeded_error => |value| value,
        else => return error.TestUnexpectedResult,
    };
    try std.testing.expectEqual(@as(i64, 2), legacy_limit_error.actual);
}

test "unsupported graph diagnostics identify the rejected operation feature" {
    const Case = struct {
        body: []const u8,
        operation: []const u8,
        feature: []const u8,
        reason: []const u8,
        message: ?[]const u8 = null,
    };
    const cases = [_]Case{
        .{
            .body = "{\"expand_strategy\":\"union\",\"graph_queries\":{}}",
            .operation = "$request",
            .feature = "expand_strategy",
            .reason = "expand_strategy_not_supported",
            .message = "expand_strategy is a legacy graph_searches result-merging control; remove it when using graph_queries and consume the named typed graph_results directly",
        },
        .{
            .body = "{\"graph_searches\":{\"walk\":{\"type\":\"traverse\",\"params\":{\"deduplicate_nodes\":false}}}}",
            .operation = "walk",
            .feature = "traverse",
            .reason = "deduplicate_nodes_must_be_true",
        },
        .{
            .body = "{\"graph_searches\":{\"walk\":{\"type\":\"traverse\",\"start_nodes\":{\"result_ref\":\"$full_text_results\"}}}}",
            .operation = "walk",
            .feature = "traverse",
            .reason = "start_selector_not_supported",
        },
        .{
            .body = "{\"graph_searches\":{\"path\":{\"type\":\"shortest_path\",\"start_nodes\":{\"keys\":[\"a\"]},\"target_nodes\":{\"result_ref\":\"$embeddings_results\"}}}}",
            .operation = "path",
            .feature = "shortest_path",
            .reason = "target_selector_not_supported",
        },
        .{
            .body = "{\"graph_searches\":{\"walk\":{\"type\":\"neighbors\",\"params\":{\"weight_mode\":\"min_weight\"}}}}",
            .operation = "walk",
            .feature = "neighbors",
            .reason = "weight_mode_must_be_min_hops",
        },
        .{
            .body = "{\"graph_searches\":{\"path\":{\"type\":\"shortest_path\",\"target_nodes\":{\"keys\":[\"b\"]},\"params\":{\"k\":2}}}}",
            .operation = "path",
            .feature = "shortest_path",
            .reason = "k_must_equal_one",
        },
        .{
            .body = "{\"graph_searches\":{\"path\":{\"type\":\"shortest_path\"}}}",
            .operation = "path",
            .feature = "shortest_path",
            .reason = "target_required",
        },
        .{
            .body = "{\"graph_searches\":{\"match\":{\"type\":\"pattern\"}}}",
            .operation = "match",
            .feature = "pattern",
            .reason = "pattern_required",
        },
        .{
            .body = "{\"graph_searches\":{\"match\":{\"type\":\"pattern\",\"pattern\":[{\"alias\":\"a\"},{\"alias\":\"b\",\"edge\":{\"direction\":\"both\"}}]}}}",
            .operation = "match",
            .feature = "pattern",
            .reason = "direction_must_be_out",
        },
    };

    for (cases) |case| {
        const body = try graphQueryUnsupportedBody(std.testing.allocator, case.body);
        defer std.testing.allocator.free(body);
        var parsed = try ant_json.parseFromSlice(
            metadata_openapi.GraphQueryUnsupportedError,
            std.testing.allocator,
            body,
            .{},
        );
        defer parsed.deinit();
        try std.testing.expectEqualStrings(case.operation, parsed.value.operation);
        try std.testing.expectEqualStrings(case.feature, parsed.value.feature);
        try std.testing.expectEqualStrings(case.reason, parsed.value.reason);
        if (case.message) |message| try std.testing.expectEqualStrings(message, parsed.value.message);
    }
}

test "runtime graph capability diagnostics preserve the failing named operation" {
    var diagnostic_context: graph_request_diagnostics.Context = .{};
    const diagnostic_scope = graph_request_diagnostics.Scope.init(&diagnostic_context);
    defer diagnostic_scope.deinit();
    graph_query_diagnostic.reset();
    graph_query_diagnostic.record(
        "later_match",
        "match",
        .external_alias_source_not_supported,
    );
    const body = try graphQueryCapabilityUnsupportedBody(
        std.testing.allocator,
        "{\"graph_queries\":{\"first_walk\":{\"index\":\"g\",\"traverse\":{\"start\":{\"keys\":[\"a\"]}}},\"later_match\":{\"index\":\"g\",\"match\":{}}}}",
        "external_alias_source_not_supported",
    );
    defer std.testing.allocator.free(body);
    var parsed = try ant_json.parseFromSlice(
        metadata_openapi.GraphQueryUnsupportedError,
        std.testing.allocator,
        body,
        .{},
    );
    defer parsed.deinit();
    try std.testing.expectEqualStrings("later_match", parsed.value.operation);
    try std.testing.expectEqualStrings("match", parsed.value.feature);
    try std.testing.expectEqualStrings("external_alias_source_not_supported", parsed.value.reason);
}

test "recorded graph diagnostics explain serverless legacy rejection" {
    var diagnostic_context: graph_request_diagnostics.Context = .{};
    const diagnostic_scope = graph_request_diagnostics.Scope.init(&diagnostic_context);
    defer diagnostic_scope.deinit();
    graph_query_diagnostic.reset();
    defer graph_query_diagnostic.reset();
    graph_query_diagnostic.record(
        "$request",
        "graph_searches",
        .legacy_graph_searches_not_supported,
    );

    const body = try graphQueryUnsupportedBody(
        std.testing.allocator,
        "{\"graph_searches\":{\"neighbors\":{\"type\":\"neighbors\"}}}",
    );
    defer std.testing.allocator.free(body);
    var parsed = try ant_json.parseFromSlice(
        metadata_openapi.GraphQueryUnsupportedError,
        std.testing.allocator,
        body,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectEqualStrings("$request", parsed.value.operation);
    try std.testing.expectEqualStrings("graph_searches", parsed.value.feature);
    try std.testing.expectEqualStrings(
        "legacy_graph_searches_not_supported",
        parsed.value.reason,
    );
    try std.testing.expectEqualStrings(
        "serverless graph queries require graph_queries; graph_searches is available only on stateful/provisioned Antfly during its compatibility window",
        parsed.value.message,
    );
}

test "recorded graph diagnostics identify unsupported request controls" {
    var diagnostic_context: graph_request_diagnostics.Context = .{};
    const diagnostic_scope = graph_request_diagnostics.Scope.init(&diagnostic_context);
    defer diagnostic_scope.deinit();
    graph_query_diagnostic.reset();
    defer graph_query_diagnostic.reset();
    graph_query_diagnostic.record(
        "$request",
        "order_by",
        .request_control_not_supported,
    );

    const body = try graphQueryUnsupportedBody(
        std.testing.allocator,
        "{\"graph_queries\":{\"walk\":{\"index\":\"g\",\"match\":{}}},\"order_by\":[{\"field\":\"created_at\"}]}",
    );
    defer std.testing.allocator.free(body);
    var parsed = try ant_json.parseFromSlice(
        metadata_openapi.GraphQueryUnsupportedError,
        std.testing.allocator,
        body,
        .{},
    );
    defer parsed.deinit();

    try std.testing.expectEqualStrings("$request", parsed.value.operation);
    try std.testing.expectEqualStrings("order_by", parsed.value.feature);
    try std.testing.expectEqualStrings("request_control_not_supported", parsed.value.reason);
    try std.testing.expectEqualStrings(
        "this request control cannot be combined with exact graph execution in this runtime; remove the field or run the graph query separately",
        parsed.value.message,
    );
}

test "public table query handler maps unsupported exact sort" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            return error.UnsupportedExactSort;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"order_by":[{"field":"created_at"}]}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 422), resp.status);
    try std.testing.expect(resp.json);
    var parsed = try std.json.parseFromSlice(struct {
        status: u16,
        @"error": []const u8,
        message: []const u8,
        reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
    }, std.testing.allocator, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u16, 422), parsed.value.status);
    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.@"error");
    try std.testing.expectEqualStrings("exact sort is unsupported for this query", parsed.value.message);
    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.reason);
    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.sort_rejection_reason);
    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.sort_rejection_detail);
    try std.testing.expectEqualStrings("", parsed.value.sort_rejection_field);
}

test "public table query handler exposes stable count-only sort rejection reason" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            db_mod.testing.recordSortRejectionDiagnostic(
                "*",
                "unsupported_exact_sort",
                "count_only_ordered_page",
            );
            return error.UnsupportedExactSort;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"count":true,"order_by":[{"field":"created_at"}]}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 422), resp.status);
    var parsed = try std.json.parseFromSlice(struct {
        status: u16,
        @"error": []const u8,
        reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
    }, std.testing.allocator, resp.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u16, 422), parsed.value.status);
    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.@"error");
    try std.testing.expectEqualStrings("count_only_ordered_page", parsed.value.reason);
    try std.testing.expectEqualStrings("count_only_ordered_page", parsed.value.sort_rejection_reason);
    try std.testing.expectEqualStrings("count_only_ordered_page", parsed.value.sort_rejection_detail);
    try std.testing.expectEqualStrings("*", parsed.value.sort_rejection_field);
}

test "public table query handler surfaces exact sort rejection diagnostics" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = executeTableQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryRequest(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: ?[]const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryError![]u8 {
            db_mod.testing.recordSortRejectionDiagnostic(
                "created_at",
                "missing_doc_values_coverage",
                "missing_doc_values_section",
            );
            return error.UnsupportedExactSort;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"order_by":[{"field":"created_at"}]}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 422), resp.status);
    var parsed = try std.json.parseFromSlice(struct {
        status: u16,
        @"error": []const u8,
        message: []const u8,
        reason: []const u8,
        sort_rejection_reason: []const u8,
        sort_rejection_detail: []const u8,
        sort_rejection_field: []const u8,
    }, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u16, 422), parsed.value.status);
    try std.testing.expectEqualStrings("unsupported_exact_sort", parsed.value.@"error");
    try std.testing.expectEqualStrings("exact sort is unsupported for this query", parsed.value.message);
    try std.testing.expectEqualStrings("field_not_sort_ready", parsed.value.reason);
    try std.testing.expectEqualStrings("field_not_sort_ready", parsed.value.sort_rejection_reason);
    try std.testing.expectEqualStrings("field_not_sort_ready", parsed.value.sort_rejection_detail);
    try std.testing.expectEqualStrings("created_at", parsed.value.sort_rejection_field);
}

test "public table query view handler maps doc identity unavailable errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = executeTableQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryView(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: TableApi.TableQueryView,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryViewError![]u8 {
            return error.DocIdentityUnavailable;
        }
    };

    var resp = try handleTableQueryView(std.testing.allocator, "docs", .latest, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try expectQueryTemporarilyUnavailableResponse(
        std.testing.allocator,
        resp,
        "doc_identity_unavailable",
        "doc identity unavailable",
    );
}

test "public table query view handler maps HA read gate errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = executeTableQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryView(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: TableApi.TableQueryView,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryViewError![]u8 {
            return error.ReadRequiresPrimary;
        }
    };

    var resp = try handleTableQueryView(std.testing.allocator, "docs", .latest, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try expectQueryTemporarilyUnavailableResponse(
        std.testing.allocator,
        resp,
        "read_requires_primary",
        "read requires primary",
    );
}

test "public table query view handler returns json response" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = executeTableQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableQueryView(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            view: TableApi.TableQueryView,
            _: operation.RequestContext,
        ) TableApi.ExecuteQueryViewError![]u8 {
            if (!std.mem.eql(u8, table_name, "docs")) return error.InternalFailure;
            if (view != .latest) return error.InternalFailure;
            return alloc.dupe(u8, "{\"table_name\":\"docs\",\"view\":\"latest\"}") catch error.InternalFailure;
        }
    };

    var resp = try handleTableQueryView(std.testing.allocator, "docs", .latest, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("{\"table_name\":\"docs\",\"view\":\"latest\"}", resp.body);
}

test "public table backup handler maps unsupported multi-range error" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = executeTableBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBackup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: backups_api.BackupFormat,
            _: ?backups_api.TableBackupFence,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteBackupError!void {
            return error.UnsupportedMultiRangeTable;
        }
    };

    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableBackup(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
        Backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("backup does not support multi-range tables", resp.body);
}

test "public table backup handler rejects an existing backup id" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = executeTableBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBackup(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: backups_api.BackupFormat,
            _: ?backups_api.TableBackupFence,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteBackupError!void {
            return error.BackupAlreadyExists;
        }
    };

    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableBackup(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
        Backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 409), resp.status);
    try ant_json.testing.expectEqualJsonText(std.testing.allocator, backups_api.backup_already_exists_body, resp.body);
}

test "public table backup handler exposes non-retryable fenced outcomes" {
    const Backend = struct {
        failure: TableApi.ExecuteBackupError,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = executeTableBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBackup(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: backups_api.BackupFormat,
            _: ?backups_api.TableBackupFence,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteBackupError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.failure;
        }
    };

    const cases = [_]struct { failure: TableApi.ExecuteBackupError, status: u16 = 409, expected: []const u8 }{
        .{ .failure = error.CatalogChanged, .expected = backups_api.catalog_changed_body },
        .{
            .failure = error.BackupOutcomeAmbiguous,
            .expected = "{\"code\":\"backup_outcome_ambiguous\",\"error\":\"backup outcome is ambiguous; inspect the backup id before retrying\",\"message\":\"backup outcome is ambiguous; inspect the backup id and artifact id before retrying\",\"retryable\":false,\"backup_id\":\"snap\"}",
        },
        .{
            .failure = error.UnsupportedBackupMigrationState,
            .status = 400,
            .expected = "{\"error\":\"backup does not support active schema migration\"}",
        },
    };
    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    for (cases) |case| {
        var backend = Backend{ .failure = case.failure };
        var resp = try handleTableBackup(
            std.testing.allocator,
            "docs",
            "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
            backend.iface(),
            null,
            &node_config,
            null,
        );
        defer resp.deinit(std.testing.allocator);
        try std.testing.expectEqual(case.status, resp.status);
        try std.testing.expect(resp.json);
        try ant_json.testing.expectEqualJsonText(std.testing.allocator, case.expected, resp.body);
    }
}

test "public table backup handler accepts portable format" {
    const Backend = struct {
        seen_portable: bool = false,
        seen_connection: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = executeTableBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableBackup(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            format: backups_api.BackupFormat,
            _: ?backups_api.TableBackupFence,
            _: []const u8,
            connection: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteBackupError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen_portable = format == .portable;
            self.seen_connection = std.mem.eql(u8, connection, "test-backups");
        }
    };

    var backend = Backend{};
    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableBackup(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\",\"format\":\"portable\"}",
        backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expect(backend.seen_portable);
    try std.testing.expect(backend.seen_connection);
}

test "public table restore handler maps target already exists" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = executeTableRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableRestore(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteRestoreError!void {
            return error.TableAlreadyExists;
        }
    };

    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableRestore(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
        Backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("restore target already exists", resp.body);
}

test "public table restore handler maps unsupported multi-range error" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = executeTableRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableRestore(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteRestoreError!void {
            return error.UnsupportedMultiRangeTable;
        }
    };

    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableRestore(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
        Backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("restore does not support multi-range tables", resp.body);
}

test "public table restore handler reports artifact integrity failures" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = executeTableRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableRestore(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteRestoreError!void {
            return error.BackupIntegrityFailure;
        }
    };

    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableRestore(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
        Backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 422), resp.status);
    try std.testing.expectEqualStrings(backups_api.integrity_failure_message, resp.body);
}

test "public table restore handler reports committed durability pending" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = executeTableRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableRestore(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteRestoreError!void {
            return error.RestoreDurabilityPending;
        }
    };

    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableRestore(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
        Backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expectEqualStrings("{\"restore\":\"committed\",\"durability\":\"pending\"}", resp.body);
}

test "public table restore handler reports confirmed durability" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = executeTableRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                },
            };
        }

        fn executeTableRestore(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: *backups_api.BackupLocation,
            _: operation.RequestContext,
        ) TableApi.ExecuteRestoreError!void {
            return error.RestoreDurabilityConfirmed;
        }
    };

    var node_config = try testBackupNodeConfig(std.testing.allocator);
    defer node_config.deinit();
    var resp = try handleTableRestore(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"connection\":\"test-backups\"}",
        Backend.iface(),
        null,
        &node_config,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("{\"restore\":\"committed\",\"durability\":\"durable\"}", resp.body);
}

test "public document artifact manifest handlers map HA read gate errors" {
    const Backend = struct {
        storage_unavailable: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                    .execute_document_artifact_manifest = executeDocumentArtifactManifest,
                    .execute_document_artifact_manifests = executeDocumentArtifactManifests,
                },
            };
        }

        fn executeDocumentArtifactManifest(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.storage_unavailable) return error.StorageReadTemporarilyUnavailable;
            return error.ReadUnavailable;
        }

        fn executeDocumentArtifactManifests(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteDocumentArtifactManifestsError!db_mod.types.DocumentArtifactManifestList {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.storage_unavailable) return error.StorageReadTemporarilyUnavailable;
            return error.ReadRequiresPrimary;
        }
    };

    var backend = Backend{};

    var manifest_resp = try handleDocumentArtifactManifest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "document_units_v1",
        .{},
        backend.iface(),
    );
    defer manifest_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), manifest_resp.status);
    try std.testing.expectEqualStrings("standby read unavailable", manifest_resp.body);

    var list_resp = try handleDocumentArtifactManifests(
        std.testing.allocator,
        "docs",
        "doc:a",
        .{},
        backend.iface(),
    );
    defer list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), list_resp.status);
    try std.testing.expectEqualStrings("read requires primary", list_resp.body);

    backend.storage_unavailable = true;
    var storage_resp = try handleDocumentArtifactManifest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "document_units_v1",
        .{},
        backend.iface(),
    );
    defer storage_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), storage_resp.status);
    try std.testing.expect(storage_resp.json);
    try std.testing.expectEqual(@as(?u32, 1), storage_resp.retry_after_seconds);
    try std.testing.expectEqualStrings(
        "{\"code\":\"storage_read_temporarily_unavailable\",\"message\":\"storage read temporarily unavailable\",\"retryable\":true}",
        storage_resp.body,
    );

    var storage_list_resp = try handleDocumentArtifactManifests(
        std.testing.allocator,
        "docs",
        "doc:a",
        .{},
        backend.iface(),
    );
    defer storage_list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), storage_list_resp.status);
    try std.testing.expect(storage_list_resp.json);
    try std.testing.expectEqual(@as(?u32, 1), storage_list_resp.retry_after_seconds);
}

test "public document artifact manifest handler returns summary and raw state" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                    .execute_document_artifact_manifest = executeDocumentArtifactManifest,
                    .execute_document_artifact_manifests = executeDocumentArtifactManifests,
                },
            };
        }

        fn makeManifest(
            alloc: std.mem.Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) TableApi.ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest {
            return .{
                .document_id = alloc.dupe(u8, doc_key) catch return error.InternalFailure,
                .artifact_name = alloc.dupe(u8, artifact_name) catch return error.InternalFailure,
                .artifact_id = alloc.dupe(u8, "af1:asset:doc:a:document_units_v1") catch return error.InternalFailure,
                .manifest_json = alloc.dupe(u8, "{\"manifest_version\":2}") catch return error.InternalFailure,
                .state_json = alloc.dupe(u8, "{\"kind\":\"document_extraction_state_v1\"}") catch return error.InternalFailure,
                .generation = 7,
                .route_type = alloc.dupe(u8, "text") catch return error.InternalFailure,
                .unit_count = 2,
                .chunk_count = 3,
                .child_range_count = 1,
                .merge_status = alloc.dupe(u8, "converged") catch return error.InternalFailure,
                .merge_operation_count = 4,
            };
        }

        fn executeDocumentArtifactManifest(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest {
            if (!std.mem.eql(u8, table_name, "docs")) return error.InternalFailure;
            if (!std.mem.eql(u8, doc_key, "doc:a")) return error.InternalFailure;
            if (!std.mem.eql(u8, artifact_name, "document_units_v1")) return error.InternalFailure;
            return makeManifest(alloc, doc_key, artifact_name);
        }

        fn executeDocumentArtifactManifests(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteDocumentArtifactManifestsError!db_mod.types.DocumentArtifactManifestList {
            if (!std.mem.eql(u8, table_name, "docs")) return error.InternalFailure;
            if (!std.mem.eql(u8, doc_key, "doc:a")) return error.InternalFailure;
            const document_id = alloc.dupe(u8, doc_key) catch return error.InternalFailure;
            errdefer alloc.free(document_id);
            var artifacts = alloc.alloc(db_mod.types.DocumentArtifactManifest, 1) catch return error.InternalFailure;
            errdefer alloc.free(artifacts);
            artifacts[0] = makeManifest(alloc, doc_key, "document_units_v1") catch return error.InternalFailure;
            return .{
                .document_id = document_id,
                .artifacts = artifacts,
            };
        }
    };

    var resp = try handleDocumentArtifactManifest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "document_units_v1",
        .{ .detail = .raw },
        Backend.iface(),
    );
    defer resp.deinit(std.testing.allocator);

    const Parsed = struct {
        document_id: []const u8,
        generation: u64,
        route_type: []const u8,
        unit_count: usize,
        manifest_json: []const u8,
        state_json: ?[]const u8,
    };
    var parsed = try std.json.parseFromSlice(Parsed, std.testing.allocator, resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqualStrings("doc:a", parsed.value.document_id);
    try std.testing.expectEqual(@as(u64, 7), parsed.value.generation);
    try std.testing.expectEqualStrings("text", parsed.value.route_type);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.unit_count);
    try std.testing.expectEqualStrings("{\"manifest_version\":2}", parsed.value.manifest_json);
    try std.testing.expectEqualStrings("{\"kind\":\"document_extraction_state_v1\"}", parsed.value.state_json.?);

    var list_resp = try handleDocumentArtifactManifests(
        std.testing.allocator,
        "docs",
        "doc:a",
        .{ .detail = .raw },
        Backend.iface(),
    );
    defer list_resp.deinit(std.testing.allocator);
    const ParsedList = struct {
        document_id: []const u8,
        artifacts: []const Parsed,
    };
    var parsed_list = try std.json.parseFromSlice(ParsedList, std.testing.allocator, list_resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed_list.deinit();

    try std.testing.expectEqual(@as(u16, 200), list_resp.status);
    try std.testing.expectEqualStrings("doc:a", parsed_list.value.document_id);
    try std.testing.expectEqual(@as(usize, 1), parsed_list.value.artifacts.len);
    try std.testing.expectEqualStrings("doc:a", parsed_list.value.artifacts[0].document_id);
    try std.testing.expectEqual(@as(u64, 7), parsed_list.value.artifacts[0].generation);

    var summary_resp = try handleDocumentArtifactManifest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "document_units_v1",
        .{ .detail = .summary },
        Backend.iface(),
    );
    defer summary_resp.deinit(std.testing.allocator);
    var parsed_summary = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, summary_resp.body, .{});
    defer parsed_summary.deinit();
    try std.testing.expect(parsed_summary.value.object.get("manifest_json") == null);
    try std.testing.expect(parsed_summary.value.object.get("state_json") == null);
}

test "public document artifact reprocess handler returns accepted" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                    .execute_reprocess_document_artifact = executeReprocessDocumentArtifact,
                },
            };
        }

        fn executeReprocessDocumentArtifact(
            _: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
            _: operation.RequestContext,
        ) TableApi.ExecuteReprocessDocumentArtifactError!void {
            if (!std.mem.eql(u8, table_name, "docs")) return error.InternalFailure;
            if (!std.mem.eql(u8, doc_key, "doc:a")) return error.InternalFailure;
            if (!std.mem.eql(u8, artifact_name, "document_units_v1")) return error.InternalFailure;
        }
    };

    var resp = try handleReprocessDocumentArtifact(
        std.testing.allocator,
        "docs",
        "doc:a",
        "document_units_v1",
        Backend.iface(),
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expectEqualStrings("{\"reprocess\":\"triggered\"}", resp.body);
}

test "public document artifact range reprocess handler returns bounded summary" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
                .request = .{},
                .vtable = &.{
                    .execute_table_batch = unsupportedBatch,
                    .execute_table_query_request = unsupportedQueryRequest,
                    .execute_table_query_view = unsupportedQueryView,
                    .execute_table_backup = unsupportedBackup,
                    .execute_table_restore = unsupportedRestore,
                    .execute_table_list_indexes = unsupportedListIndexes,
                    .execute_table_get_index = unsupportedGetIndex,
                    .execute_table_create_index = unsupportedCreateIndex,
                    .execute_table_delete_index = unsupportedDeleteIndex,
                    .execute_reprocess_document_artifact_range = executeReprocessDocumentArtifactRange,
                },
            };
        }

        fn executeReprocessDocumentArtifactRange(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            req: db_mod.types.DocumentArtifactTableReprocessRequest,
            _: operation.RequestContext,
        ) TableApi.ExecuteReprocessDocumentArtifactRangeError!db_mod.types.DocumentArtifactTableReprocessResult {
            if (!std.mem.eql(u8, table_name, "docs")) return error.InternalFailure;
            if (!std.mem.eql(u8, artifact_name, "document_units_v1")) return error.InternalFailure;
            if (!std.mem.eql(u8, req.from_key, "doc:a")) return error.InternalFailure;
            if (req.shard_cursors.len != 1) return error.InternalFailure;
            if (req.shard_cursors[0].group_id != 42) return error.InternalFailure;
            if (!std.mem.eql(u8, req.shard_cursors[0].next_key, "doc:b")) return error.InternalFailure;
            if (req.shard_cursors[0].limit != 10) return error.InternalFailure;
            if (req.limit != 10) return error.InternalFailure;
            const failures = alloc.alloc(db_mod.types.DocumentArtifactReprocessFailure, 1) catch return error.InternalFailure;
            failures[0] = .{
                .key = alloc.dupe(u8, "doc:b") catch return error.InternalFailure,
                .error_code = alloc.dupe(u8, "InvalidDataUri") catch return error.InternalFailure,
            };
            const shard_cursors = alloc.alloc(db_mod.types.DocumentArtifactReprocessShardCursor, 1) catch return error.InternalFailure;
            shard_cursors[0] = .{
                .group_id = 42,
                .next_key = alloc.dupe(u8, "doc:b") catch return error.InternalFailure,
                .scanned = 2,
                .reprocessed = 1,
                .skipped = 0,
                .failed = 1,
                .limit = 10,
            };
            return .{
                .scanned = 2,
                .reprocessed = 1,
                .skipped = 0,
                .failed = 1,
                .limit = 10,
                .next_key = alloc.dupe(u8, "doc:b") catch return error.InternalFailure,
                .failures = failures,
                .shard_cursors = shard_cursors,
            };
        }
    };

    var resp = try handleReprocessDocumentArtifactRange(
        std.testing.allocator,
        "docs",
        "document_units_v1",
        "{\"from_key\":\"doc:a\",\"limit\":10,\"shard_cursors\":[{\"group_id\":42,\"next_key\":\"doc:b\",\"limit\":10}]}",
        Backend.iface(),
    );
    defer resp.deinit(std.testing.allocator);

    const Parsed = struct {
        reprocess: []const u8,
        reprocess_status: []const u8,
        artifact_name: []const u8,
        scanned: usize,
        reprocessed: usize,
        failed: usize,
        next_key: ?[]const u8,
        pending_shards: usize,
        failures: []const struct {
            key: []const u8,
            error_code: []const u8,
        },
        shard_cursors: []const struct {
            group_id: ?u64,
            next_key: []const u8,
            scanned: usize,
            reprocessed: usize,
            failed: usize,
            limit: u32,
        },
    };
    var parsed = try std.json.parseFromSlice(Parsed, std.testing.allocator, resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expectEqualStrings("triggered", parsed.value.reprocess);
    try std.testing.expectEqualStrings("in_progress", parsed.value.reprocess_status);
    try std.testing.expectEqualStrings("document_units_v1", parsed.value.artifact_name);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.scanned);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.reprocessed);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.failed);
    try std.testing.expectEqualStrings("doc:b", parsed.value.next_key.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.pending_shards);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.failures.len);
    try std.testing.expectEqualStrings("doc:b", parsed.value.failures[0].key);
    try std.testing.expectEqualStrings("InvalidDataUri", parsed.value.failures[0].error_code);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.shard_cursors.len);
    try std.testing.expectEqual(@as(?u64, 42), parsed.value.shard_cursors[0].group_id);
    try std.testing.expectEqualStrings("doc:b", parsed.value.shard_cursors[0].next_key);
    try std.testing.expectEqual(@as(usize, 2), parsed.value.shard_cursors[0].scanned);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.shard_cursors[0].reprocessed);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.shard_cursors[0].failed);
    try std.testing.expectEqual(@as(u32, 10), parsed.value.shard_cursors[0].limit);
}
