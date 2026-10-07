// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const native_platform = @import("antfly_platform");
const std = @import("std");

const docstore = @import("docstore.zig");

test "lite generation readers use each transaction checkpoint for external values" {
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/external-snapshots.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    var store = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    const old_value = try alloc.dupe(u8, &(@as([16384]u8, @splat('a'))));
    defer alloc.free(old_value);
    const new_value = try alloc.dupe(u8, &(@as([32768]u8, @splat('b'))));
    defer alloc.free(new_value);
    var write = try store.beginWrite();
    try write.put("doc", old_value);
    try write.commit();
    var unopened_reader = try docstore.Store.openWithOptions(alloc, path, .{ .read_only = true, .io = native_platform.testing.io });
    defer unopened_reader.close();
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
    var delayed = try unopened_reader.beginRead();
    defer delayed.abort();
    try std.testing.expectEqualStrings(old_value, try delayed.get("doc"));
    try std.testing.expectEqualStrings(old_value, try pinned.get("doc"));
    try std.testing.expectEqualStrings(new_value, try fresh.get("doc"));
}

test "lite online checks validate the pinned file length while later commits append" {
    const native = @import("native.zig");
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/online-check.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    var store = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = native_platform.testing.io });
    defer store.close();
    try store.file.putDocument("doc", "before");
    var snapshot = try native.NativeFile.openWithIo(alloc, native_platform.testing.io, path, .{ .read_only = true });
    defer snapshot.close();
    const size = (try snapshot.file.stat(native_platform.testing.io)).size;
    try store.file.putDocument("doc", "after");
    try std.testing.expect((try snapshot.checkAtFileSizeWithCancel(size, null)).valid);
    try std.testing.expect((try store.checkWithCancel(null)).valid);
    // A tail already present when the check pins its header remains corruption.
    try store.file.file.writePositionalAll(native_platform.testing.io, "bad tail", (try store.file.file.stat(native_platform.testing.io)).size);
    const invalid = try store.checkWithCancel(null);
    try std.testing.expect(!invalid.valid);
    try std.testing.expectEqualStrings("tail_bytes", invalid.issue.?);
}

test "lite vacuum publication resets private fallback sequences to the final image" {
    const native = @import("native.zig");
    const alloc = std.testing.allocator;
    var tmp = native_platform.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/vacuum-sequences.aflite", .{tmp.sub_path});
    defer alloc.free(path);
    var file = try native.NativeFile.createWithIo(alloc, native_platform.testing.io, path, .{ .no_sync = true });
    defer file.close();
    try file.putDocument("doc", "before");
    var image = try file.prepareVacuum(null);
    defer image.deinit();
    for (0..4) |_| try image.prepared.putDocument("doc", "intermediate");
    try image.prepared.putDocument("doc", "final");
    try image.prepared.preparePublicationSequence(file.activeCheckpoint().commit_sequence + 1);
    try file.publishVacuum(&image);
    var reopened = try native.NativeFile.openWithIo(alloc, native_platform.testing.io, path, .{ .read_only = true });
    defer reopened.close();
    const value = (try reopened.getDocumentAlloc(alloc, "doc")).?;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("final", value);
    try std.testing.expect((try reopened.check()).valid);
}
