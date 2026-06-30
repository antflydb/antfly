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
        MethodNotAllowed,
        InternalFailure,
    };

    pub const ExecuteRestoreError = error{
        InvalidRequest,
        TableAlreadyExists,
        MethodNotAllowed,
        InternalFailure,
    };

    pub const VTable = struct {
        execute_cluster_backup_list: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            location_uri: []const u8,
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
        ) ExecuteRestoreError![]u8,
    };

    pub fn executeClusterBackupList(
        self: ClusterApi,
        alloc: std.mem.Allocator,
        location_uri: []const u8,
    ) ExecuteListError![]u8 {
        return try self.vtable.execute_cluster_backup_list(self.ptr, alloc, location_uri);
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
    ) ExecuteRestoreError![]u8 {
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
    api: ClusterApi,
) !OwnedResponse {
    const body = api.executeClusterBackupList(alloc, location_uri) catch |err| switch (err) {
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
) !OwnedResponse {
    var req = backups_api.parseClusterBackupRequest(alloc, body) catch |err| switch (err) {
        error.UnsupportedBackupFormat => return .{ .status = 400, .body = try alloc.dupe(u8, "unsupported backup format") },
        else => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid backup request") },
    };
    defer backups_api.freeClusterBackupRequest(alloc, &req);

    var location = backups_api.openBackupLocationWithSecrets(alloc, req.location, secret_store) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    const response_body = api.executeClusterBackup(alloc, req, &location) catch |err| switch (err) {
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
) !OwnedResponse {
    var req = backups_api.parseClusterRestoreRequest(alloc, body) catch |err| switch (err) {
        else => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") },
    };
    defer backups_api.freeClusterRestoreRequest(alloc, &req);

    var location = backups_api.openBackupLocationWithSecrets(alloc, req.location, secret_store) catch |err| {
        if (backups_api.backupLocationErrorMessage(err)) |msg| {
            return .{ .status = 400, .body = try alloc.dupe(u8, msg) };
        }
        return err;
    };
    defer location.deinit(alloc);

    const restore_mode = backups_api.validateClusterRestoreMode(req.restore_mode) catch {
        return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") };
    };

    const response_body = api.executeClusterRestore(alloc, req, &location, restore_mode) catch |err| switch (err) {
        error.InvalidRequest => return .{ .status = 400, .body = try alloc.dupe(u8, "invalid restore request") },
        error.TableAlreadyExists => return .{ .status = 400, .body = try alloc.dupe(u8, "table already exists") },
        error.MethodNotAllowed => return .{ .status = 405, .body = try alloc.dupe(u8, "method not allowed") },
        error.InternalFailure => return .{ .status = 500, .body = try alloc.dupe(u8, "restore failed") },
    };
    return .{ .status = 202, .body = response_body };
}
