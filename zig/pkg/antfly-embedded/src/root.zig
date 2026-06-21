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
const embedded = @import("embedded_surface");
pub const db = embedded.db;
pub const api = embedded.api;
pub const host_environment = embedded.host_environment;
pub const object_storage = embedded.object_storage;
pub const lsm_backend = embedded.lsm_backend;
pub const storage_backend = embedded.storage_backend;
pub const db_types = embedded.db_types;

test "pkg antfly embedded root compiles" {
    _ = db;
    _ = api;
    _ = host_environment;
    _ = object_storage;
    _ = lsm_backend;
    _ = storage_backend;
    _ = db_types;
    _ = db.Capabilities;
    _ = db.InferenceOpenOptions;
    _ = db.InferenceStatus;
    _ = db.LiteCheckReport;
    _ = db.LiteStableSnapshotReport;
    _ = db.LiteStatus;
    _ = db.LiteStorageStatus;
    _ = db.LiteVacuumReport;
    _ = db.capabilitiesForProfile;
    _ = db.checkLiteFile;
    _ = db.copyStableLiteSnapshotFile;
    _ = api.checkLiteFileJson;
    _ = api.copyStableLiteSnapshotFileJson;
}

test "pkg antfly embedded exposes Lite path-level check helpers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/pkg-lite-malformed.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{});
        defer file.close(std.testing.io);
        try file.writePositionalAll(std.testing.io, "short embedded package header", 0);
    }

    const report: db.LiteCheckReport = try db.checkLiteFile(allocator, path);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("truncated_header", report.issue.?);

    const json = try api.checkLiteFileJson(allocator, path);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"valid\":false") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"issue\":\"truncated_header\"") != null);
}

test "pkg antfly embedded exposes Lite path-level snapshot helpers" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/pkg-lite-snapshot-src.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const snapshot_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/pkg-lite-snapshot-copy.aflite", .{tmp.sub_path});
    defer allocator.free(snapshot_path);

    {
        var lite = try db.DB.createLite(allocator, path, .{});
        defer lite.close();
        try lite.batch(.{
            .writes = &.{.{
                .key = "doc:pkg-lite-snapshot",
                .value = "{\"title\":\"embedded package snapshot\"}",
            }},
            .sync_level = .write,
        });
    }

    const report: db.LiteStableSnapshotReport = try db.copyStableLiteSnapshotFile(allocator, path, snapshot_path, false);
    try std.testing.expect(report.snapshot_size > 0);
    try std.testing.expect(report.page_count > 0);

    const json = try api.copyStableLiteSnapshotFileJson(allocator, path, snapshot_path, true);
    defer allocator.free(json);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"snapshot_size\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "\"checkpoint_sequence\":") != null);

    var snapshot = try db.DB.openLite(allocator, snapshot_path, .{
        .open_mode = .query_readonly,
    });
    defer snapshot.close();

    var result = (try snapshot.lookup(allocator, "doc:pkg-lite-snapshot", .{})) orelse return error.MissingLiteSnapshotDocument;
    defer result.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, result.value, "embedded package snapshot") != null);
}

test "pkg antfly embedded exposes Lite handle-level snapshot helper" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/pkg-lite-handle-snapshot-src.aflite", .{tmp.sub_path});
    defer allocator.free(path);
    const snapshot_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/pkg-lite-handle-snapshot-copy.aflite", .{tmp.sub_path});
    defer allocator.free(snapshot_path);

    {
        var lite = try db.DB.createLite(allocator, path, .{});
        defer lite.close();
        try lite.batch(.{
            .writes = &.{.{
                .key = "doc:pkg-lite-handle-snapshot",
                .value = "{\"title\":\"embedded package handle snapshot\"}",
            }},
            .sync_level = .write,
        });

        const report: db.LiteStableSnapshotReport = try lite.copyStableLiteSnapshot(snapshot_path, false);
        try std.testing.expect(report.snapshot_size > 0);
        try std.testing.expect(report.page_count > 0);
    }

    var snapshot = try db.DB.openLite(allocator, snapshot_path, .{
        .open_mode = .query_readonly,
    });
    defer snapshot.close();

    var result = (try snapshot.lookup(allocator, "doc:pkg-lite-handle-snapshot", .{})) orelse return error.MissingLiteHandleSnapshotDocument;
    defer result.deinit(allocator);
    try std.testing.expect(std.mem.indexOf(u8, result.value, "embedded package handle snapshot") != null);
}
