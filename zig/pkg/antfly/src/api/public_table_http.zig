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
const builtin = @import("builtin");
const metadata_openapi = @import("antfly_metadata_openapi");
const backups_api = @import("backups.zig");
const batch_api = @import("batch.zig");
const db_mod = @import("../storage/db/mod.zig");
const common_secrets = @import("../common/secrets.zig");
const common_config = @import("../common/config.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const query_contract = @import("query_contract.zig");

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

    pub const ExecuteBatchError = error{
        InvalidBatchRequest,
        UnsupportedSyncLevel,
        NotFound,
        MethodNotAllowed,
        Backpressured,
        Unavailable,
        WriteUnavailable,
        DocIdentityUnavailable,
        HAReadOnlyStandby,
        HAPromotedStandbyRequiresPrimaryOpen,
        HAFencedPrimary,
        InternalFailure,
    };

    pub const ExecuteQueryError = error{
        InvalidQueryRequest,
        NotFound,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        ModelNotFound,
        UnsupportedExactSort,
        QueryCandidateBudgetExceeded,
        InternalFailure,
    };

    pub const ExecuteQueryViewError = error{
        NotFound,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        ModelNotFound,
        InternalFailure,
    };

    pub const ExecuteBackupError = error{
        NotFound,
        MethodNotAllowed,
        UnsupportedBackupMigrationState,
        UnsupportedMultiRangeTable,
        InternalFailure,
    };

    pub const ExecuteRestoreError = error{
        NotLeader,
        TableAlreadyExists,
        MethodNotAllowed,
        UnsupportedBackupMigrationState,
        UnsupportedBackupFormat,
        InvalidBackupRequest,
        InternalFailure,
    };

    pub const ExecuteListIndexesError = error{
        NotFound,
        InternalFailure,
    };

    pub const ExecuteGetIndexError = error{
        NotFound,
        InternalFailure,
    };

    pub const ExecuteCreateIndexError = error{
        NotLeader,
        NotFound,
        MethodNotAllowed,
        InvalidIndexRequest,
        ProbeUnavailable,
        ModelNotFound,
        InternalFailure,
    };

    pub const ExecuteDeleteIndexError = error{
        NotLeader,
        NotFound,
        MethodNotAllowed,
        InternalFailure,
    };

    pub const ExecutePutArtifactEnrichmentError = error{
        NotLeader,
        NotFound,
        MethodNotAllowed,
        InvalidEnrichmentRequest,
        InternalFailure,
    };

    pub const ExecuteDeleteArtifactEnrichmentError = error{
        NotLeader,
        NotFound,
        MethodNotAllowed,
        InvalidEnrichmentRequest,
        InternalFailure,
    };

    pub const ExecuteListArtifactEnrichmentsError = error{
        NotFound,
        InternalFailure,
    };

    pub const ExecuteDocumentArtifactManifestError = error{
        NotFound,
        MethodNotAllowed,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        InternalFailure,
    };

    pub const ExecuteDocumentArtifactManifestsError = error{
        NotFound,
        MethodNotAllowed,
        DocIdentityUnavailable,
        ReadRequiresPrimary,
        ReadUnavailable,
        InternalFailure,
    };

    pub const ExecuteReprocessDocumentArtifactError = error{
        NotFound,
        MethodNotAllowed,
        DocIdentityUnavailable,
        InternalFailure,
    };

    pub const ExecuteReprocessDocumentArtifactRangeError = error{
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
        ) ExecuteBatchError!void,
        execute_table_query_request: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            body: []const u8,
            row_filter_json: ?[]const u8,
        ) ExecuteQueryError![]u8,
        execute_table_query_view: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            view: TableQueryView,
        ) ExecuteQueryViewError![]u8,
        execute_table_backup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            backup_id: []const u8,
            format: backups_api.BackupFormat,
            location_uri: []const u8,
            location: *backups_api.BackupLocation,
        ) ExecuteBackupError!void,
        execute_table_restore: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            backup_id: []const u8,
            location_uri: []const u8,
            location: *backups_api.BackupLocation,
        ) ExecuteRestoreError!void,
        execute_table_list_indexes: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) ExecuteListIndexesError![]u8,
        execute_table_get_index: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
        ) ExecuteGetIndexError![]u8,
        execute_table_create_index: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            body: []const u8,
        ) ExecuteCreateIndexError!void,
        execute_table_delete_index: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
        ) ExecuteDeleteIndexError!void,
        execute_put_artifact_enrichment: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            body: []const u8,
        ) ExecutePutArtifactEnrichmentError!void = null,
        execute_delete_artifact_enrichment: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
        ) ExecuteDeleteArtifactEnrichmentError!void = null,
        execute_list_artifact_enrichments: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
        ) ExecuteListArtifactEnrichmentsError![]u8 = null,
        execute_document_artifact_manifest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest = null,
        execute_document_artifact_manifests: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
        ) ExecuteDocumentArtifactManifestsError!db_mod.types.DocumentArtifactManifestList = null,
        execute_reprocess_document_artifact: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) ExecuteReprocessDocumentArtifactError!void = null,
        execute_reprocess_document_artifact_range: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            artifact_name: []const u8,
            req: db_mod.types.DocumentArtifactTableReprocessRequest,
        ) ExecuteReprocessDocumentArtifactRangeError!db_mod.types.DocumentArtifactTableReprocessResult = null,
    };

    pub fn executeTableBatch(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
    ) ExecuteBatchError!void {
        return try self.vtable.execute_table_batch(self.ptr, alloc, table_name, req);
    }

    pub fn executeTableQueryRequest(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        body: []const u8,
        row_filter_json: ?[]const u8,
    ) ExecuteQueryError![]u8 {
        return try self.vtable.execute_table_query_request(self.ptr, alloc, table_name, body, row_filter_json);
    }

    pub fn executeTableQueryView(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        view: TableQueryView,
    ) ExecuteQueryViewError![]u8 {
        return try self.vtable.execute_table_query_view(self.ptr, alloc, table_name, view);
    }

    pub fn executeTableBackup(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        format: backups_api.BackupFormat,
        location_uri: []const u8,
        location: *backups_api.BackupLocation,
    ) ExecuteBackupError!void {
        return try self.vtable.execute_table_backup(self.ptr, alloc, table_name, backup_id, format, location_uri, location);
    }

    pub fn executeTableRestore(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        backup_id: []const u8,
        location_uri: []const u8,
        location: *backups_api.BackupLocation,
    ) ExecuteRestoreError!void {
        return try self.vtable.execute_table_restore(self.ptr, alloc, table_name, backup_id, location_uri, location);
    }

    pub fn executeTableListIndexes(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) ExecuteListIndexesError![]u8 {
        return try self.vtable.execute_table_list_indexes(self.ptr, alloc, table_name);
    }

    pub fn executeTableGetIndex(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) ExecuteGetIndexError![]u8 {
        return try self.vtable.execute_table_get_index(self.ptr, alloc, table_name, index_name);
    }

    pub fn executeTableCreateIndex(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
        body: []const u8,
    ) ExecuteCreateIndexError!void {
        return try self.vtable.execute_table_create_index(self.ptr, alloc, table_name, index_name, body);
    }

    pub fn executeTableDeleteIndex(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        index_name: []const u8,
    ) ExecuteDeleteIndexError!void {
        return try self.vtable.execute_table_delete_index(self.ptr, alloc, table_name, index_name);
    }

    pub fn executePutArtifactEnrichment(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        body: []const u8,
    ) ExecutePutArtifactEnrichmentError!void {
        const fn_ptr = self.vtable.execute_put_artifact_enrichment orelse return error.MethodNotAllowed;
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name, body);
    }

    pub fn executeDeleteArtifactEnrichment(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
    ) ExecuteDeleteArtifactEnrichmentError!void {
        const fn_ptr = self.vtable.execute_delete_artifact_enrichment orelse return error.MethodNotAllowed;
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name);
    }

    pub fn executeListArtifactEnrichments(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) ExecuteListArtifactEnrichmentsError![]u8 {
        const fn_ptr = self.vtable.execute_list_artifact_enrichments orelse return error.NotFound;
        return try fn_ptr(self.ptr, alloc, table_name);
    }

    pub fn executeDocumentArtifactManifest(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest {
        const fn_ptr = self.vtable.execute_document_artifact_manifest orelse return error.MethodNotAllowed;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, artifact_name);
    }

    pub fn executeDocumentArtifactManifests(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
    ) ExecuteDocumentArtifactManifestsError!db_mod.types.DocumentArtifactManifestList {
        const fn_ptr = self.vtable.execute_document_artifact_manifests orelse return error.MethodNotAllowed;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key);
    }

    pub fn executeReprocessDocumentArtifact(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) ExecuteReprocessDocumentArtifactError!void {
        const fn_ptr = self.vtable.execute_reprocess_document_artifact orelse return error.MethodNotAllowed;
        return try fn_ptr(self.ptr, alloc, table_name, doc_key, artifact_name);
    }

    pub fn executeReprocessDocumentArtifactRange(
        self: TableApi,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        artifact_name: []const u8,
        req: db_mod.types.DocumentArtifactTableReprocessRequest,
    ) ExecuteReprocessDocumentArtifactRangeError!db_mod.types.DocumentArtifactTableReprocessResult {
        const fn_ptr = self.vtable.execute_reprocess_document_artifact_range orelse return error.MethodNotAllowed;
        return try fn_ptr(self.ptr, alloc, table_name, artifact_name, req);
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

    pub fn deinit(self: *OwnedResponse, alloc: std.mem.Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

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

pub fn handleTableBatch(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    api: TableApi,
) !OwnedResponse {
    var batch_req = batch_api.parseBatchRequest(alloc, body) catch |err| {
        switch (err) {
            error.ValueTooLong => return .{ .status = 413, .body = try alloc.dupe(u8, "value too large") },
            else => return err,
        }
    };
    defer batch_req.deinit(alloc);

    api.executeTableBatch(alloc, table_name, batch_req.req) catch |err| switch (err) {
        error.InvalidBatchRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid batch request") },
        error.UnsupportedSyncLevel => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported sync_level") },
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.Backpressured => return .{ .status = 429, .body = try alloc.dupe(u8, "table backpressured") },
        error.Unavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "maintenance routes unavailable on query-only runtime") },
        error.WriteUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "write unavailable") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.HAReadOnlyStandby => return .{ .status = 409, .body = try alloc.dupe(u8, "standby is read-only") },
        error.HAPromotedStandbyRequiresPrimaryOpen => return .{ .status = 409, .body = try alloc.dupe(u8, "promoted standby requires primary open") },
        error.HAFencedPrimary => return .{ .status = 409, .body = try alloc.dupe(u8, "fenced primary rejects writes") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "batch failed") },
    };

    return .{
        .status = 201,
        .body = try batch_api.encodeBatchResponse(alloc, batch_req.result()),
    };
}

pub fn handleTableQueryRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    row_filter_json: ?[]const u8,
    api: TableApi,
) !OwnedResponse {
    if (try bodyHasInternalShardQueryFields(alloc, body)) {
        std.log.warn("public table query rejected internal fields table={s}", .{table_name});
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid query request") };
    }

    db_mod.resetLastSortRejectionDiagnostic();
    query_contract.validatePublicQuerySortTupleContract(alloc, body) catch |err| switch (err) {
        error.InvalidQueryRequest => {
            std.log.warn("public table query invalid exact sort table={s} err={}", .{ table_name, err });
            return .{ .status = 422, .body = try unsupportedExactSortBody(alloc) };
        },
    };
    const response_body = api.executeTableQueryRequest(alloc, table_name, body, row_filter_json) catch |err| {
        switch (err) {
            error.InvalidQueryRequest => {
                if (db_mod.peekLastSortRejectionDiagnostic() != null) {
                    std.log.warn("public table query invalid exact sort table={s} err={}", .{ table_name, err });
                    return .{ .status = 422, .body = try unsupportedExactSortBody(alloc) };
                }
                std.log.err("public table query invalid table={s} err={}", .{ table_name, err });
                return .{ .status = 400, .body = try alloc.dupe(u8, "invalid query request") };
            },
            error.NotFound => {
                std.log.err("public table query missing table={s} err={}", .{ table_name, err });
                return .{ .status = 404, .body = try alloc.dupe(u8, "not found") };
            },
            error.DocIdentityUnavailable => {
                std.log.warn("public table query doc identity unavailable table={s} err={}", .{ table_name, err });
                return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") };
            },
            error.ReadRequiresPrimary => {
                std.log.warn("public table query requires primary table={s} err={}", .{ table_name, err });
                return .{ .status = 503, .body = try alloc.dupe(u8, "read requires primary") };
            },
            error.ReadUnavailable => {
                std.log.warn("public table query standby unavailable table={s} err={}", .{ table_name, err });
                return .{ .status = 503, .body = try alloc.dupe(u8, "standby read unavailable") };
            },
            error.ModelNotFound => {
                std.log.warn("public table query model not found table={s} err={}", .{ table_name, err });
                return .{ .status = 404, .body = try alloc.dupe(u8, "{\"error\":\"MODEL_NOT_FOUND\",\"message\":\"model not found\"}") };
            },
            error.QueryCandidateBudgetExceeded => {
                std.log.warn("public table query candidate budget exceeded table={s} err={}", .{ table_name, err });
                return .{ .status = 422, .body = try queryCandidateBudgetExceededBody(alloc) };
            },
            error.UnsupportedExactSort => {
                std.log.warn("public table query unsupported exact sort table={s} err={}", .{ table_name, err });
                return .{ .status = 422, .body = try unsupportedExactSortBody(alloc) };
            },
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
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.ReadRequiresPrimary => return .{ .status = 503, .body = try alloc.dupe(u8, "read requires primary") },
        error.ReadUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "standby read unavailable") },
        error.ModelNotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "{\"error\":\"MODEL_NOT_FOUND\",\"message\":\"model not found\"}") },
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
    }) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    api.executeTableBackup(alloc, table_name, parsed_req.value.backup_id, backup_format, parsed_req.value.location, &location) catch |err| switch (err) {
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.UnsupportedBackupMigrationState => return .{ .status = 400, .body = try alloc.dupe(u8, "backup does not support active schema migration") },
        error.UnsupportedMultiRangeTable => return .{ .status = 400, .body = try alloc.dupe(u8, "backup does not support multi-range tables") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "backup failed") },
    };

    return .{
        .status = 201,
        .body = try backups_api.encodeBackupSuccess(alloc),
    };
}

fn parseBackupFormat(value: ?[]const u8) !backups_api.BackupFormat {
    const format = value orelse return .native;
    if (std.mem.eql(u8, format, "native")) return .native;
    if (std.mem.eql(u8, format, "portable")) return .portable;
    return error.UnsupportedBackupFormat;
}

pub fn handleTableRestore(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    api: TableApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
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
    }) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    api.executeTableRestore(alloc, table_name, parsed_req.value.backup_id, parsed_req.value.location, &location) catch |err| switch (err) {
        error.NotLeader => return err,
        error.TableAlreadyExists => return .{ .status = 400, .body = try alloc.dupe(u8, "restore target already exists") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.UnsupportedBackupMigrationState => return .{ .status = 400, .body = try alloc.dupe(u8, "restore does not support active schema migration") },
        error.UnsupportedBackupFormat => return .{ .status = 400, .body = try alloc.dupe(u8, "restore does not support this backup layout") },
        error.InvalidBackupRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "restore failed") },
    };

    return .{
        .status = 202,
        .body = try backups_api.encodeRestoreTriggered(alloc),
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
    );
    defer restore.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore.status);
    try std.testing.expectEqualStrings("invalid restore request", restore.body);
}

pub fn handleTableListIndexes(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    const response_body = api.executeTableListIndexes(alloc, table_name) catch |err| switch (err) {
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index list failed") },
    };
    return .{ .status = 200, .body = response_body };
}

pub fn handleTableGetIndex(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    const response_body = api.executeTableGetIndex(alloc, table_name, index_name) catch |err| switch (err) {
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index lookup failed") },
    };
    return .{ .status = 200, .body = response_body };
}

pub fn handleTableCreateIndex(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    body: []const u8,
    api: TableApi,
) !OwnedResponse {
    api.executeTableCreateIndex(alloc, table_name, index_name, body) catch |err| switch (err) {
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InvalidIndexRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported index configuration") },
        error.ProbeUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "index validation probe unavailable") },
        error.ModelNotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "{\"error\":\"MODEL_NOT_FOUND\",\"message\":\"model not found\"}") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index create failed") },
    };
    return .{ .status = 201, .body = try alloc.dupe(u8, "{}") };
}

pub fn handleTableDeleteIndex(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    api.executeTableDeleteIndex(alloc, table_name, index_name) catch |err| switch (err) {
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index delete failed") },
    };
    return .{ .status = 201, .body = try alloc.dupe(u8, "{}") };
}

pub fn handlePutArtifactEnrichment(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    artifact_name: []const u8,
    body: []const u8,
    api: TableApi,
) !OwnedResponse {
    api.executePutArtifactEnrichment(alloc, table_name, artifact_name, body) catch |err| switch (err) {
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
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
        error.NotLeader => return err,
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
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
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact enrichment list failed") },
    };
    return .{ .status = 200, .body = response_body };
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
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.ReadRequiresPrimary => return .{ .status = 503, .body = try alloc.dupe(u8, "read requires primary") },
        error.ReadUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "standby read unavailable") },
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
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.ReadRequiresPrimary => return .{ .status = 503, .body = try alloc.dupe(u8, "read requires primary") },
        error.ReadUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "standby read unavailable") },
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
) TableApi.ExecuteBatchError!void {
    return error.InternalFailure;
}

fn unsupportedQueryRequest(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: ?[]const u8,
) TableApi.ExecuteQueryError![]u8 {
    return error.InternalFailure;
}

fn unsupportedQueryView(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: TableApi.TableQueryView,
) TableApi.ExecuteQueryViewError![]u8 {
    return error.InternalFailure;
}

fn unsupportedBackup(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: backups_api.BackupFormat,
    _: []const u8,
    _: *backups_api.BackupLocation,
) TableApi.ExecuteBackupError!void {
    return error.InternalFailure;
}

fn unsupportedListIndexes(
    _: *anyopaque,
    alloc: std.mem.Allocator,
    _: []const u8,
) TableApi.ExecuteListIndexesError![]u8 {
    _ = alloc;
    return error.InternalFailure;
}

fn unsupportedGetIndex(
    _: *anyopaque,
    alloc: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
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
) TableApi.ExecuteCreateIndexError!void {
    return error.InternalFailure;
}

fn unsupportedDeleteIndex(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
) TableApi.ExecuteDeleteIndexError!void {
    return error.InternalFailure;
}

fn unsupportedRestore(
    _: *anyopaque,
    _: std.mem.Allocator,
    _: []const u8,
    _: []const u8,
    _: []const u8,
    _: *backups_api.BackupLocation,
) TableApi.ExecuteRestoreError!void {
    return error.InternalFailure;
}

test "public table batch handler returns created batch response" {
    const Backend = struct {
        called: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
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
    var parsed = try std.json.parseFromSlice(struct { inserted: ?i64 = null }, std.testing.allocator, resp.body, .{});
    defer parsed.deinit();

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expect(backend.called);
    try std.testing.expectEqual(@as(i64, 1), parsed.value.inserted.?);
}

test "public table batch handler maps backend errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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

test "public table batch handler maps unavailable errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
        ) TableApi.ExecuteBatchError!void {
            return error.WriteUnavailable;
        }
    };

    var resp = try handleTableBatch(std.testing.allocator, "docs",
        \\{"inserts":{"doc-a":{"title":"alpha"}}}
    , Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("write unavailable", resp.body);
}

test "public table batch handler maps doc identity unavailable errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
        ) TableApi.ExecuteQueryError![]u8 {
            return error.DocIdentityUnavailable;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}}}
    , null, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("doc identity unavailable", resp.body);
}

test "public table query handler maps HA read gate errors" {
    const Backend = struct {
        err: TableApi.ExecuteQueryError,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
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
    try std.testing.expectEqualStrings("read requires primary", primary_resp.body);

    var lag_backend = Backend{ .err = error.ReadUnavailable };
    var lag_resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}}}
    , null, lag_backend.iface());
    defer lag_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), lag_resp.status);
    try std.testing.expectEqualStrings("standby read unavailable", lag_resp.body);
}

test "public table query handler returns json response" {
    const Backend = struct {
        called: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
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
        ) TableApi.ExecuteQueryError![]u8 {
            return error.InternalFailure;
        }
    };

    var resp = try handleTableQueryRequest(std.testing.allocator, "docs",
        \\{"query":{"match_all":{}},"order_by":[{"field":"created_at","descc":true}]}
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

test "public table query handler maps unsupported exact sort" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
        ) TableApi.ExecuteQueryError![]u8 {
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
        ) TableApi.ExecuteQueryViewError![]u8 {
            return error.DocIdentityUnavailable;
        }
    };

    var resp = try handleTableQueryView(std.testing.allocator, "docs", .latest, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("doc identity unavailable", resp.body);
}

test "public table query view handler maps HA read gate errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
        ) TableApi.ExecuteQueryViewError![]u8 {
            return error.ReadRequiresPrimary;
        }
    };

    var resp = try handleTableQueryView(std.testing.allocator, "docs", .latest, Backend.iface());
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("read requires primary", resp.body);
}

test "public table query view handler returns json response" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
            _: []const u8,
            _: *backups_api.BackupLocation,
        ) TableApi.ExecuteBackupError!void {
            return error.UnsupportedMultiRangeTable;
        }
    };

    var resp = try handleTableBackup(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\"}",
        Backend.iface(),
        null,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("backup does not support multi-range tables", resp.body);
}

test "public table backup handler accepts portable format" {
    const Backend = struct {
        seen_portable: bool = false,

        fn iface(self: *@This()) TableApi {
            return .{
                .ptr = self,
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
            _: []const u8,
            _: *backups_api.BackupLocation,
        ) TableApi.ExecuteBackupError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen_portable = format == .portable;
        }
    };

    var backend = Backend{};
    var resp = try handleTableBackup(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\",\"format\":\"portable\"}",
        backend.iface(),
        null,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expect(backend.seen_portable);
}

test "public table restore handler maps target already exists" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
            _: *backups_api.BackupLocation,
        ) TableApi.ExecuteRestoreError!void {
            return error.TableAlreadyExists;
        }
    };

    var resp = try handleTableRestore(
        std.testing.allocator,
        "docs",
        "{\"backup_id\":\"snap\",\"location\":\"file:///tmp/out\"}",
        Backend.iface(),
        null,
        null,
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("restore target already exists", resp.body);
}

test "public document artifact manifest handlers map HA read gate errors" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
            _: []const u8,
        ) TableApi.ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest {
            return error.ReadUnavailable;
        }

        fn executeDocumentArtifactManifests(
            _: *anyopaque,
            _: std.mem.Allocator,
            _: []const u8,
            _: []const u8,
        ) TableApi.ExecuteDocumentArtifactManifestsError!db_mod.types.DocumentArtifactManifestList {
            return error.ReadRequiresPrimary;
        }
    };

    var manifest_resp = try handleDocumentArtifactManifest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "document_units_v1",
        .{},
        Backend.iface(),
    );
    defer manifest_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), manifest_resp.status);
    try std.testing.expectEqualStrings("standby read unavailable", manifest_resp.body);

    var list_resp = try handleDocumentArtifactManifests(
        std.testing.allocator,
        "docs",
        "doc:a",
        .{},
        Backend.iface(),
    );
    defer list_resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 503), list_resp.status);
    try std.testing.expectEqualStrings("read requires primary", list_resp.body);
}

test "public document artifact manifest handler returns summary and raw state" {
    const Backend = struct {
        fn iface() TableApi {
            return .{
                .ptr = undefined,
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
