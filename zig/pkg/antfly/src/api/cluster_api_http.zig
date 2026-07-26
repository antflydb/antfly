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
        NotLeader,
        InvalidRequest,
        NoTables,
        BackupAlreadyExists,
        BackupRepositoryBusy,
        BackupManifestTooLarge,
        MethodNotAllowed,
        InternalFailure,
    };

    pub const ExecuteRestoreError = error{
        NotLeader,
        InvalidRequest,
        BackupRepositoryBusy,
        BackupManifestTooLarge,
        BackupIntegrityFailure,
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
        ) ExecuteBackupError![]u8,
        execute_cluster_restore: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            req: backups_api.ClusterRestoreRequest,
            location: *backups_api.BackupLocation,
            restore_mode: []const u8,
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
    ) ExecuteBackupError![]u8 {
        return try self.vtable.execute_cluster_backup(self.ptr, alloc, req, location);
    }

    pub fn executeClusterRestore(
        self: ClusterApi,
        alloc: std.mem.Allocator,
        req: backups_api.ClusterRestoreRequest,
        location: *backups_api.BackupLocation,
        restore_mode: []const u8,
    ) ExecuteRestoreError!RestoreExecution {
        return try self.vtable.execute_cluster_restore(self.ptr, alloc, req, location, restore_mode);
    }
};

pub const OwnedResponse = struct {
    status: u16,
    body: []u8,

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
    return .{ .status = 200, .body = body };
}

pub fn handleClusterBackup(
    alloc: std.mem.Allocator,
    body: []const u8,
    api: ClusterApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
) !OwnedResponse {
    var req = backups_api.parseClusterBackupRequest(alloc, body) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") };
    };
    defer backups_api.freeClusterBackupRequest(alloc, &req);
    if (req.connection == null) {
        return .{ .status = 400, .body = try alloc.dupe(u8, "backup requires a named external_io connection") };
    }

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

    const response_body = api.executeClusterBackup(alloc, req, &location) catch |err| switch (err) {
        error.NotLeader => return err,
        error.InvalidRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") },
        error.NoTables => return .{ .status = 400, .body = try alloc.dupe(u8, "no tables to backup") },
        error.BackupAlreadyExists => return .{ .status = 409, .body = try alloc.dupe(u8, "backup id already exists") },
        error.BackupRepositoryBusy => return .{ .status = 409, .body = try alloc.dupe(u8, "backup repository is busy; retry later") },
        error.BackupManifestTooLarge => return .{ .status = 400, .body = try alloc.dupe(u8, backups_api.manifest_too_large_message) },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "backup failed") },
    };
    return .{ .status = 200, .body = response_body };
}

pub fn handleClusterRestore(
    alloc: std.mem.Allocator,
    body: []const u8,
    api: ClusterApi,
    secret_store: ?*common_secrets.FileStore,
    node_config: ?*const common_config.Config,
    io: ?std.Io,
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

    const result = api.executeClusterRestore(alloc, req, &location, restore_mode) catch |err| switch (err) {
        error.NotLeader => return err,
        error.InvalidRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") },
        error.BackupRepositoryBusy => return .{ .status = 409, .body = try alloc.dupe(u8, "backup repository is busy; retry later") },
        error.BackupManifestTooLarge => return .{ .status = 400, .body = try alloc.dupe(u8, backups_api.manifest_too_large_message) },
        error.BackupIntegrityFailure => return .{ .status = 422, .body = try alloc.dupe(u8, backups_api.integrity_failure_message) },
        error.TableAlreadyExists => return .{ .status = 400, .body = try alloc.dupe(u8, "table already exists") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.Cancelled => return .{ .status = 409, .body = try alloc.dupe(u8, "restore cancelled") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "restore failed") },
    };
    return .{ .status = result.status, .body = result.body };
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
    );
    defer restore.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 400), restore.status);
    try std.testing.expectEqualStrings("invalid restore request", restore.body);
}
