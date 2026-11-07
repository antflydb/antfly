// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Real cluster upgrade barrier, before a durable public restore admission.
//! Portable UNLOGGED data is never omitted or relabeled an empty generation.
const std = @import("std");
const local = @import("antfly_local_sources");
const backups = @import("backups.zig");
const A = std.mem.Allocator;
pub fn needed(manifest: *const backups.TableBackupManifest) bool {
    return manifest.format == .portable and manifest.persistence_kind == .unlogged and manifest.content_policy == .complete;
}
pub fn require(a: A, server: anytype, input: local.api_operation.RequestContext, manifest: *const backups.TableBackupManifest) !void {
    if (!needed(manifest)) return;
    var context = input;
    context.setting_admin = true;
    try context.ensureActive();
    const bytes = try server.source.systemCatalog(a, context, .{ .sql_relation_boot_read = .{ .required_data_version = @import("../metadata/table_manager.zig").native_nonempty_restore_protocol_version } });
    defer a.free(bytes);
    var observed = try std.json.parseFromSlice(@import("../metadata/sql_relation_boots.zig").Observation, a, bytes, .{});
    defer observed.deinit();
    if (observed.value.data_version_ready != true) return error.NativeArtifactRestoreUpgradeRequired;
}
pub fn requireCluster(a: A, server: anytype, context: local.api_operation.RequestContext, location: *backups.BackupLocation, manifest: *const backups.ClusterBackupManifest, selected: ?[]const []const u8) !void {
    for (manifest.tables) |entry| {
        try context.ensureActive();
        if (selected) |names| {
            const included = for (names) |name| {
                if (std.mem.eql(u8, name, entry.name)) break true;
            } else false;
            if (!included) continue;
        }
        var table = try backups.readManifestFromLocationWithArtifactBackupId(a, location, entry.table_backup_id, entry.artifact_backup_id orelse entry.table_backup_id);
        defer table.deinit(a);
        if (needed(&table)) {
            try require(a, server, context, &table);
            return;
        }
    }
}

test "native artifact restore admission refuses missing or false upgrade proof and leaves physical schema only path unchanged" {
    const FakeSource = struct {
        calls: usize = 0,
        missing: bool = false,
        pub fn systemCatalog(self: *@This(), a: A, context: local.api_operation.RequestContext, query: anytype) ![]u8 {
            self.calls += 1;
            try std.testing.expect(context.setting_admin);
            try std.testing.expectEqual(@import("../metadata/table_manager.zig").native_nonempty_restore_protocol_version, query.sql_relation_boot_read.required_data_version);
            return a.dupe(u8, if (self.missing) "{\"record\":null,\"registration_current\":false}" else "{\"record\":null,\"registration_current\":false,\"data_version_ready\":false}");
        }
    };
    var source: FakeSource = .{};
    var server = .{ .source = &source };
    var manifest: backups.TableBackupManifest = .{ .format_version = 3, .format = .portable, .content_policy = .complete, .persistence_kind = .unlogged, .backup_id = "daily", .table_name = "scratch", .table_id = 9, .description = "", .schema_json = "{}", .read_schema_json = "", .indexes_json = "{}", .replication_sources_json = "[]", .shards = &.{} };
    try std.testing.expectError(error.NativeArtifactRestoreUpgradeRequired, require(std.testing.allocator, &server, .{}, &manifest));
    source.missing = true;
    try std.testing.expectError(error.NativeArtifactRestoreUpgradeRequired, require(std.testing.allocator, &server, .{}, &manifest));
    try std.testing.expectEqual(@as(usize, 2), source.calls);
    manifest.format = .native;
    manifest.content_policy = .schema_only;
    try require(std.testing.allocator, &server, .{}, &manifest);
    try std.testing.expectEqual(@as(usize, 2), source.calls);
}
