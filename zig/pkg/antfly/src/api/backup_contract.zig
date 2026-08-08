// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

//! Data-only backup/restore contract shared across compiled runtime units.
//! Remote-store implementations and backup algorithms stay in backups.zig.

const std = @import("std");

pub const format_version: u32 = 2;

pub const BackupFormat = enum {
    native,
    portable,
};

pub const ArtifactIntegrityMode = enum {
    declared,
    derive_after_materialization,
};

pub const ShardSnapshot = struct {
    group_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8 = null,
    snapshot_path: []const u8,
    artifact_size_bytes: u64 = 0,
    artifact_sha256: []const u8 = "",

    pub fn deinit(self: ShardSnapshot, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.start_key));
        if (self.end_key) |value| alloc.free(@constCast(value));
        alloc.free(@constCast(self.snapshot_path));
        if (self.artifact_sha256.len > 0) alloc.free(@constCast(self.artifact_sha256));
    }
};

pub const TableBackupManifest = struct {
    format_version: u32 = format_version,
    format: BackupFormat,
    artifact_integrity_mode: ArtifactIntegrityMode = .declared,
    backup_id: []const u8,
    table_name: []const u8,
    description: []const u8,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
    replication_sources_json: []const u8,
    shards: []const ShardSnapshot,

    pub fn deinit(self: *TableBackupManifest, alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.backup_id));
        alloc.free(@constCast(self.table_name));
        alloc.free(@constCast(self.description));
        alloc.free(@constCast(self.schema_json));
        alloc.free(@constCast(self.read_schema_json));
        alloc.free(@constCast(self.indexes_json));
        alloc.free(@constCast(self.replication_sources_json));
        for (self.shards) |shard| shard.deinit(alloc);
        alloc.free(@constCast(self.shards));
        self.* = undefined;
    }
};

pub const RestorePublicationHook = struct {
    ptr: *anyopaque,
    publish_definition: *const fn (ptr: *anyopaque) anyerror!void,
    rollback_definition: *const fn (ptr: *anyopaque) anyerror!void,

    pub fn publish(self: @This()) !void {
        try self.publish_definition(self.ptr);
    }

    pub fn rollback(self: @This()) !void {
        try self.rollback_definition(self.ptr);
    }
};

pub const TableBackupPlan = struct {
    backup_root: []const u8,
    backup_id: []const u8,
    format: BackupFormat = .native,
    io: ?std.Io = null,
};

pub const TableRestorePlan = struct {
    backup_root: []const u8,
    manifest: *const TableBackupManifest,
    artifact_backup_id: []const u8,
    source_location: []const u8,
    reconcile_only: bool = false,
    replace_existing: bool = false,
    publication_hook: ?RestorePublicationHook = null,
    io: ?std.Io = null,
};
