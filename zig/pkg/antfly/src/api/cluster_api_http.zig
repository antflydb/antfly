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
const backups_api = @import("backups.zig");
const operation = @import("operation.zig");
const common_secrets = @import("../common/secrets.zig");
const common_config = @import("../common/config.zig");

pub const ClusterApi = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const ExecuteListError = error{
        InvalidRequest,
        UnsupportedBackupLocation,
        MethodNotAllowed,
        InternalFailure,
    };

    pub const ExecuteBackupError = error{
        Canceled,
        DeadlineExceeded,
        MetadataCapabilityUnavailable,
        NotLeader,
        InvalidRequest,
        NoTables,
        BackupAlreadyExists,
        BackupRepositoryBusy,
        BackupOutcomeAmbiguous,
        BackupManifestTooLarge,
        MethodNotAllowed,
        InternalFailure,
    };

    pub const ExecuteRestoreError = error{
        NotLeader,
        InvalidRequest,
        BackupRepositoryBusy,
        RestoreValidationPending,
        BackupManifestTooLarge,
        BackupIntegrityFailure,
        RestoreDestinationReauthorizationRequired,
        UnsupportedArtifactIndexSources,
        ArtifactIndexSourcesTemporarilyUnavailable,
        TableAlreadyExists,
        MethodNotAllowed,
        Cancelled,
        InternalFailure,
    };

    pub const RestoreExecution = struct {
        status: u16,
        body: []u8,
    };

    pub const VTable = struct {
        execute_cluster_backup_list: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            location_uri: []const u8,
            location: *backups_api.BackupLocation,
            options: backups_api.BackupListOptions,
        ) ExecuteListError![]u8,
        execute_cluster_backup: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: backups_api.ClusterBackupRequest,
            location: *backups_api.BackupLocation,
            request: operation.RequestContext,
        ) ExecuteBackupError![]u8,
        execute_cluster_restore: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: backups_api.ClusterRestoreRequest,
            location: *backups_api.BackupLocation,
            restore_mode: []const u8,
            request: operation.RequestContext,
        ) ExecuteRestoreError!RestoreExecution,
    };

    pub fn executeClusterBackupList(
        self: ClusterApi,
        alloc: std.mem.Allocator,
        location_uri: []const u8,
        location: *backups_api.BackupLocation,
        options: backups_api.BackupListOptions,
    ) ExecuteListError![]u8 {
        return try self.vtable.execute_cluster_backup_list(self.ptr, alloc, location_uri, location, options);
    }

    pub fn executeClusterBackup(
        self: ClusterApi,
        alloc: std.mem.Allocator,
        req: backups_api.ClusterBackupRequest,
        location: *backups_api.BackupLocation,
        request: operation.RequestContext,
    ) ExecuteBackupError![]u8 {
        return try self.vtable.execute_cluster_backup(self.ptr, alloc, req, location, request);
    }

    pub fn executeClusterRestore(
        self: ClusterApi,
        alloc: std.mem.Allocator,
        req: backups_api.ClusterRestoreRequest,
        location: *backups_api.BackupLocation,
        restore_mode: []const u8,
        request: operation.RequestContext,
    ) ExecuteRestoreError!RestoreExecution {
        return try self.vtable.execute_cluster_restore(self.ptr, alloc, req, location, restore_mode, request);
    }
};

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

pub fn handleClusterBackupList(
    alloc: std.mem.Allocator,
    location_uri: []const u8,
    connection: ?[]const u8,
    api: ClusterApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
    options: backups_api.BackupListOptions,
) !OwnedResponse {
    if (connection == null) {
        return .{ .status = 400, .body = try alloc.dupe(u8, "backup listing requires a named external_io connection") };
    }
    var location = backups_api.openBackupLocationWithOptions(alloc, location_uri, .{
        .secret_store = secret_store,
        .node_config = node_config,
        .connection = connection,
        .required_capability = "restore.read",
        .io = io,
    }) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        return err;
    };
    defer location.deinit(alloc);
    const body = api.executeClusterBackupList(alloc, location_uri, &location, options) catch |err| switch (err) {
        error.InvalidRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") },
        error.UnsupportedBackupLocation => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported backup location") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "backup list failed") },
    };
    return .{ .status = 200, .body = body, .json = true };
}

pub fn handleClusterBackup(
    alloc: std.mem.Allocator,
    body: []const u8,
    api: ClusterApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
    request: operation.RequestContext,
) !OwnedResponse {
    try request.ensureActive();
    var req = backups_api.parseClusterBackupRequest(alloc, body) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") };
    };
    defer backups_api.freeClusterBackupRequest(alloc, &req);
    if (req.connection == null) {
        return .{ .status = 400, .body = try alloc.dupe(u8, "backup requires a named external_io connection") };
    }

    try request.ensureActive();
    var location = backups_api.openBackupLocationWithOptions(alloc, req.location, .{
        .secret_store = secret_store,
        .node_config = node_config,
        .connection = req.connection,
        .required_capability = "backup.write",
        .io = io,
    }) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    const response_body = api.executeClusterBackup(alloc, req, &location, request) catch |err| switch (err) {
        error.Canceled, error.DeadlineExceeded => return err,
        error.MetadataCapabilityUnavailable => return .{
            .status = 503,
            .body = try alloc.dupe(u8, backups_api.metadata_capability_unavailable_body),
            .json = true,
            .retry_after_seconds = backups_api.metadata_capability_retry_after_seconds,
        },
        error.NotLeader => return err,
        error.InvalidRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") },
        error.NoTables => return .{ .status = 400, .body = try alloc.dupe(u8, "no tables to backup") },
        error.BackupAlreadyExists => return .{ .status = 409, .body = try alloc.dupe(u8, "backup id already exists") },
        error.BackupRepositoryBusy => return .{ .status = 409, .body = try alloc.dupe(u8, "backup repository is busy; retry later") },
        error.BackupOutcomeAmbiguous => return .{
            .status = 409,
            .body = try backups_api.encodeBackupOutcomeAmbiguousBody(alloc, req.backup_id, null),
            .json = true,
        },
        error.BackupManifestTooLarge => return .{ .status = 400, .body = try alloc.dupe(u8, backups_api.manifest_too_large_message) },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "backup failed") },
    };
    return .{ .status = 200, .body = response_body, .json = true };
}

pub fn handleClusterRestore(
    alloc: std.mem.Allocator,
    body: []const u8,
    api: ClusterApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
    request: operation.RequestContext,
) !OwnedResponse {
    var req = backups_api.parseClusterRestoreRequest(alloc, body) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") };
    };
    defer backups_api.freeClusterRestoreRequest(alloc, &req);
    if (req.connection == null) {
        return .{ .status = 400, .body = try alloc.dupe(u8, "restore requires a named external_io connection") };
    }

    var location = backups_api.openBackupLocationWithOptions(alloc, req.location, .{
        .secret_store = secret_store,
        .node_config = node_config,
        .connection = req.connection,
        .required_capability = "restore.read",
        .io = io,
    }) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    const restore_mode = backups_api.validateClusterRestoreMode(req.restore_mode) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") };
    };

    const result = api.executeClusterRestore(alloc, req, &location, restore_mode, request) catch |err| switch (err) {
        error.NotLeader => return err,
        error.InvalidRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") },
        error.BackupRepositoryBusy => return .{ .status = 409, .body = try alloc.dupe(u8, "backup repository is busy; retry later") },
        error.RestoreValidationPending => return .{ .status = 503, .body = try alloc.dupe(u8, "restore validation is temporarily unavailable; retry later"), .retry_after_seconds = 1 },
        error.BackupManifestTooLarge => return .{ .status = 400, .body = try alloc.dupe(u8, backups_api.manifest_too_large_message) },
        error.BackupIntegrityFailure => return .{ .status = 422, .body = try alloc.dupe(u8, backups_api.integrity_failure_message) },
        error.RestoreDestinationReauthorizationRequired => return .{ .status = 409, .body = try alloc.dupe(u8, "restore destination authorization is missing or revoked; resubmit with a currently authorized credential") },
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
        error.TableAlreadyExists => return .{ .status = 400, .body = try alloc.dupe(u8, "table already exists") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.Cancelled => return .{ .status = 409, .body = try alloc.dupe(u8, "restore cancelled") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "restore failed") },
    };
    return .{
        .status = result.status,
        .body = result.body,
        .json = result.status >= 200 and result.status < 300,
    };
}

test "cluster backup APIs require named connections" {
    var list = try handleClusterBackupList(std.testing.allocator, "s3://archive", null, undefined, null, null, null, .{});
    defer list.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), list.status);

    var backup = try handleClusterBackup(
        std.testing.allocator,
        "{\"backup_id\":\"snap\",\"location\":\"s3://archive/snap\"}",
        undefined,
        null,
        null,
        null,
        .{},
    );
    defer backup.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), backup.status);
    try std.testing.expectEqualStrings("invalid backup request", backup.body);

    var restore = try handleClusterRestore(
        std.testing.allocator,
        "{\"backup_id\":\"snap\",\"location\":\"s3://archive/snap\"}",
        undefined,
        null,
        null,
        null,
        .{},
    );
    defer restore.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore.status);
    try std.testing.expectEqualStrings("invalid restore request", restore.body);
}

test "cluster backup vtable preserves request context and canceled ingress stops before parsing" {
    const Fake = struct {
        called: bool = false,
        context_matches: bool = false,

        fn execute(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: backups_api.ClusterBackupRequest,
            _: *backups_api.BackupLocation,
            request: operation.RequestContext,
        ) ClusterApi.ExecuteBackupError![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.called = true;
            self.context_matches = std.mem.eql(u8, "snap", req.backup_id) and
                std.mem.eql(u8, "request-7", request.request_id) and
                request.deadline_ns == std.math.maxInt(u64);
            return alloc.dupe(u8, "ok") catch return error.InternalFailure;
        }
    };

    var fake = Fake{};
    const api = ClusterApi{
        .ptr = &fake,
        .vtable = &.{
            .execute_cluster_backup_list = undefined,
            .execute_cluster_backup = Fake.execute,
            .execute_cluster_restore = undefined,
        },
    };
    const req = backups_api.ClusterBackupRequest{
        .backup_id = "snap",
        .location = "file:///unused",
        .connection = "test",
    };
    const body = try api.executeClusterBackup(std.testing.allocator, req, undefined, .{
        .deadline_ns = std.math.maxInt(u64),
        .request_id = "request-7",
    });
    defer std.testing.allocator.free(body);
    try std.testing.expect(fake.called);
    try std.testing.expect(fake.context_matches);
    try std.testing.expectEqualStrings("ok", body);

    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, handleClusterBackup(
        std.testing.allocator,
        "not json",
        undefined,
        null,
        null,
        null,
        .{ .cancellation = operation.CancellationToken.fromAtomic(&cancelled) },
    ));
}
