// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const docstore = @import("docstore.zig");

test "lite generation readers use each transaction checkpoint for external values" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/external-snapshots.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    var store = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
    defer store.close();
    const old_value = try alloc.dupe(u8, &(@as([16384]u8, @splat('a'))));
    defer alloc.free(old_value);
    const new_value = try alloc.dupe(u8, &(@as([32768]u8, @splat('b'))));
    defer alloc.free(new_value);
    var write = try store.beginWrite();
    try write.put("doc", old_value);
    try write.commit();
    var pinned = try store.beginRead();
    defer pinned.abort();
    write = try store.beginWrite();
    try write.put("doc", new_value);
    try write.commit();
    var fresh = try store.beginRead();
    defer fresh.abort();
    try std.testing.expectEqualStrings(new_value, try fresh.get("doc"));
    var values: [1]?[]const u8 = undefined;
    try fresh.getManySorted(&.{"doc"}, &values);
    try std.testing.expectEqualStrings(new_value, values[0].?);
    var cursor = try fresh.openCursor();
    defer cursor.close();
    const first = try cursor.first();
    try std.testing.expectEqualStrings(new_value, first.value);
    _ = try store.vacuum();
    try std.testing.expectEqualStrings(old_value, try pinned.get("doc"));
    try std.testing.expectEqualStrings(new_value, try fresh.get("doc"));
}
