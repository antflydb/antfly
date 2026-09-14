// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const antfly = @import("../cli_root.zig");
const lite_restore_staging = @import("../standalone/restore_staging_bridge.zig");
const portable_backup = antfly.portable_backup;

test "restore input plan stages aflite as portable table restore" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    const src_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-input-plan-src.aflite", .{tmp.sub_path});
    defer allocator.free(src_path);
    const restored_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/restore-input-plan-normal-db", .{tmp.sub_path});
    defer allocator.free(restored_path);
    const cwd_tmp = try std.Io.Dir.cwd().realPathFileAlloc(io, ".zig-cache/tmp", allocator);
    defer allocator.free(cwd_tmp);
    const backup_root = try std.fmt.allocPrint(allocator, "{s}/{s}/restore-input-plan-backups", .{ cwd_tmp, tmp.sub_path });
    defer allocator.free(backup_root);
    const location = try std.fmt.allocPrint(allocator, "file://{s}", .{backup_root});
    defer allocator.free(location);

    const schema_json =
        \\{"version":0,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true}}}}
    ;

    {
        var lite = try antfly.lite.connection.Connection.create(allocator, src_path, true);
        defer lite.close();
        const db = &lite.db;
        try db.setSchemaJson(allocator, schema_json);
        try db.addEnrichment(.{
            .name = "restore_input_chunks_v1",
            .kind = .chunk,
            .field = "body",
            .chunk_size = 96,
            .chunk_overlap = 12,
        });
        try db.addIndex(.{
            .name = "restore_input_ft_body",
            .kind = .full_text,
            .config_json = "{\"chunk_name\":\"restore_input_chunks_v1\"}",
        });
        try db.batch(.{
            .writes = &.{.{
                .key = "doc:restore-input",
                .value = "{\"title\":\"restore input document\",\"body\":\"normal restore input staging\"}",
            }},
            .sync_level = .full_index,
        });
        try db.runUntilIdle();
    }

    var plan = try @import("../cmd/cli/backup.zig").prepareInputRestorePlan(allocator, src_path, "docs", null, location, "local-reader");
    defer plan.deinit(allocator);

    try std.testing.expectEqualStrings("docs", plan.tableName());
    try std.testing.expectEqualStrings("lite-restore-input-plan-src", plan.request.backup_id);
    try std.testing.expectEqualStrings(location, plan.request.location);
    try std.testing.expectEqualStrings("lite-restore-input-plan-src.afb", plan.staged.snapshot_path);

    var backup_location = try antfly.public_api.backups.openBackupLocation(allocator, location);
    defer backup_location.deinit(allocator);
    var manifest = try antfly.public_api.backups.readManifestFromLocation(allocator, &backup_location, plan.request.backup_id);
    defer manifest.deinit(allocator);

    try std.testing.expectEqualStrings("docs", manifest.table_name);
    try std.testing.expectEqualStrings(plan.request.backup_id, manifest.backup_id);
    try std.testing.expectEqualStrings(schema_json, manifest.schema_json);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_input_ft_body\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"enrichments\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, manifest.indexes_json, "\"restore_input_chunks_v1\"") != null);
    try std.testing.expectEqual(@as(usize, 1), manifest.shards.len);
    try std.testing.expectEqualStrings(plan.staged.snapshot_path, manifest.shards[0].snapshot_path);

    const afb_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ backup_root, plan.staged.snapshot_path });
    defer allocator.free(afb_path);
    const portable = try std.Io.Dir.cwd().readFileAlloc(io, afb_path, allocator, .limited(lite_restore_staging.max_afb_file_bytes));
    defer allocator.free(portable);
    try portable_backup.validatePortable(allocator, portable);

    var restored = try antfly.db.DB.open(allocator, restored_path, .{});
    defer restored.close();
    try portable_backup.importPortable(allocator, restored.core.store, portable);
    const value = (try restored.get(allocator, "doc:restore-input")) orelse return error.TestExpectedEqual;
    defer allocator.free(value);
    try std.testing.expect(std.mem.indexOf(u8, value, "restore input document") != null);
}
