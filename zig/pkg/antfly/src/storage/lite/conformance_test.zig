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
const backend_erased = @import("../backend_erased.zig");
const docstore = @import("docstore.zig");

const Allocator = std.mem.Allocator;

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

fn expectBoundStoreConformance(runtime: *backend_erased.Store) !void {
    const caps = runtime.capabilities();
    try std.testing.expect(caps.cursors);
    try std.testing.expect(caps.ordered_ranges);
    try std.testing.expect(caps.reverse_ranges);
    try std.testing.expect(caps.single_writer);
    try std.testing.expect(!caps.native_namespaces);
    try std.testing.expect(caps.write_batches == .atomic);
    try std.testing.expect(caps.read_snapshots == .snapshot);

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:b", "B");
        try txn.put("doc:a", "A");
        try txn.commit();
    }

    {
        var batch = try runtime.beginBatch();
        try batch.put("doc:c", "C");
        try batch.delete("doc:b");
        try batch.commit();
    }

    {
        var snapshot = try runtime.beginRead();
        defer snapshot.abort();

        var writer = try runtime.beginWrite();
        try writer.put("doc:a", "A2");
        try writer.commit();

        try std.testing.expectEqualStrings("A", try snapshot.get("doc:a"));
        try std.testing.expectError(error.NotFound, snapshot.get("doc:b"));
        try std.testing.expectEqualStrings("C", try snapshot.get("doc:c"));
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A2", try txn.get("doc:a"));
        try std.testing.expectError(error.NotFound, txn.get("doc:b"));
        try std.testing.expectEqualStrings("C", try txn.get("doc:c"));

        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
        try std.testing.expectEqualStrings("doc:c", (try cur.next()).?.key);
        try std.testing.expect((try cur.next()) == null);
        try std.testing.expectEqualStrings("doc:c", (try cur.last()).?.key);
        try std.testing.expectEqualStrings("doc:a", (try cur.seekAtOrBefore("doc:b")).?.key);
        try std.testing.expectEqualStrings("doc:c", (try cur.seekAtOrAfter("doc:b")).?.key);
    }
}

fn seedDurableState(runtime: *backend_erased.Store) !void {
    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:a", "A");
        try txn.put("doc:b", "B");
        try txn.commit();
    }

    {
        var batch = try runtime.beginBatch();
        try batch.delete("doc:b");
        try batch.put("doc:c", "C");
        try batch.commit();
    }
}

fn expectReopenedBoundState(runtime: *backend_erased.Store) !void {
    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("A", try txn.get("doc:a"));
    try std.testing.expectError(error.NotFound, txn.get("doc:b"));
    try std.testing.expectEqualStrings("C", try txn.get("doc:c"));

    var cur = try txn.openCursor();
    defer cur.close();
    try std.testing.expectEqualStrings("doc:a", (try cur.first()).?.key);
    try std.testing.expectEqualStrings("doc:c", (try cur.next()).?.key);
    try std.testing.expect((try cur.next()) == null);
    try std.testing.expectEqualStrings("doc:c", (try cur.last()).?.key);
}

test "storage.lite native docstore conforms to bound backend contract" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-conformance.aflite");
    defer allocator.free(path);

    var store = try docstore.Store.create(allocator, path, true);
    defer store.close();

    var runtime = try store.runtimeStore(allocator);
    defer runtime.deinit();
    try expectBoundStoreConformance(&runtime);
}

test "storage.lite native docstore conformance survives reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-conformance-reopen.aflite");
    defer allocator.free(path);

    {
        var store = try docstore.Store.create(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();
        try seedDurableState(&runtime);
    }

    {
        var store = try docstore.Store.open(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();
        try expectReopenedBoundState(&runtime);
    }
}
