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
        DocIdentityUnavailable,
        InternalFailure,
    };

    pub const ExecuteQueryError = error{
        InvalidQueryRequest,
        NotFound,
        DocIdentityUnavailable,
        InternalFailure,
    };

    pub const ExecuteQueryViewError = error{
        NotFound,
        DocIdentityUnavailable,
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
        NotFound,
        MethodNotAllowed,
        InvalidIndexRequest,
        ProbeUnavailable,
        InternalFailure,
    };

    pub const ExecuteDeleteIndexError = error{
        NotFound,
        MethodNotAllowed,
        InternalFailure,
    };

    pub const ExecuteDocumentArtifactManifestError = error{
        NotFound,
        MethodNotAllowed,
        DocIdentityUnavailable,
        InternalFailure,
    };

    pub const ExecuteReprocessDocumentArtifactError = error{
        NotFound,
        MethodNotAllowed,
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
        execute_document_artifact_manifest: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) ExecuteDocumentArtifactManifestError!db_mod.types.DocumentArtifactManifest = null,
        execute_reprocess_document_artifact: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            table_name: []const u8,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) ExecuteReprocessDocumentArtifactError!void = null,
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
        location: *backups_api.BackupLocation,
    ) ExecuteBackupError!void {
        return try self.vtable.execute_table_backup(self.ptr, alloc, table_name, backup_id, location);
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
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
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

    const response_body = api.executeTableQueryRequest(alloc, table_name, body, row_filter_json) catch |err| {
        switch (err) {
            error.InvalidQueryRequest => {
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
) !OwnedResponse {
    const parsed_req = backups_api.parseBackupRequest(alloc, body) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") };
    };
    defer parsed_req.deinit();

    var location = backups_api.openBackupLocationWithSecrets(alloc, parsed_req.value.location, secret_store) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    api.executeTableBackup(alloc, table_name, parsed_req.value.backup_id, &location) catch |err| switch (err) {
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

pub fn handleTableRestore(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    body: []const u8,
    api: TableApi,
    secret_store: ?*common_secrets.FileStore,
) !OwnedResponse {
    const parsed_req = backups_api.parseRestoreRequest(alloc, body) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") };
    };
    defer parsed_req.deinit();

    var location = backups_api.openBackupLocationWithSecrets(alloc, parsed_req.value.location, secret_store) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    api.executeTableRestore(alloc, table_name, parsed_req.value.backup_id, parsed_req.value.location, &location) catch |err| switch (err) {
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
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InvalidIndexRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported index configuration") },
        error.ProbeUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "index validation probe unavailable") },
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
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "index delete failed") },
    };
    return .{ .status = 201, .body = try alloc.dupe(u8, "{}") };
}

pub fn handleDocumentArtifactManifest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    doc_key: []const u8,
    artifact_name: []const u8,
    api: TableApi,
) !OwnedResponse {
    var manifest = api.executeDocumentArtifactManifest(alloc, table_name, doc_key, artifact_name) catch |err| switch (err) {
        error.NotFound => return .{ .status = 404, .body = try alloc.dupe(u8, "not found") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.DocIdentityUnavailable => return .{ .status = 503, .body = try alloc.dupe(u8, "doc identity unavailable") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "artifact manifest lookup failed") },
    };
    defer manifest.deinit(alloc);

    const Response = struct {
        document_id: []const u8,
        artifact_name: []const u8,
        artifact_id: []const u8,
        generation: u64,
        route_type: []const u8,
        unsupported_reason: ?[]const u8,
        unit_count: usize,
        chunk_count: usize,
        child_range_count: usize,
        merge_status: []const u8,
        merge_operation_count: usize,
        manifest_json: []const u8,
        state_json: ?[]const u8,
    };
    return .{
        .status = 200,
        .body = try std.json.Stringify.valueAlloc(alloc, Response{
            .document_id = manifest.document_id,
            .artifact_name = manifest.artifact_name,
            .artifact_id = manifest.artifact_id,
            .generation = manifest.generation,
            .route_type = manifest.route_type,
            .unsupported_reason = manifest.unsupported_reason,
            .unit_count = manifest.unit_count,
            .chunk_count = manifest.chunk_count,
            .child_range_count = manifest.child_range_count,
            .merge_status = manifest.merge_status,
            .merge_operation_count = manifest.merge_operation_count,
            .manifest_json = manifest.manifest_json,
            .state_json = manifest.state_json,
        }, .{}),
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
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("backup does not support multi-range tables", resp.body);
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
    );
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("restore target already exists", resp.body);
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
                },
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
    };

    var resp = try handleDocumentArtifactManifest(
        std.testing.allocator,
        "docs",
        "doc:a",
        "document_units_v1",
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
