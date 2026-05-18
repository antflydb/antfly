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
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const mem_backend = @import("mem_backend.zig");
const lsm_backend = @import("lsm_backend/mod.zig");
const lmdb_backend = @import("lmdb_backend.zig");

fn expectNamespaceStoreConformance(
    runtime: *backend_erased.NamespaceStore,
    expect_native_namespaces: bool,
) !void {
    const caps = runtime.capabilities();
    try std.testing.expect(caps.ordered_ranges);
    try std.testing.expect(caps.single_writer);
    try std.testing.expect(caps.read_snapshots == .snapshot);
    try std.testing.expectEqual(expect_native_namespaces, caps.native_namespaces);

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "meta:lsn", "1");
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.put(.{ .name = "other" }, "doc:a", "B");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("1", try txn.get(.{}, "meta:lsn"));
        try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectEqualStrings("B", try txn.get(.{ .name = "other" }, "doc:a"));
    }

    {
        var snapshot = try runtime.beginRead();
        defer snapshot.abort();

        var writer = try runtime.beginWrite();
        try writer.put(.{ .name = "docs" }, "doc:a", "A2");
        try writer.delete(.{ .name = "other" }, "doc:a");
        try writer.commit();

        try std.testing.expectEqualStrings("A", try snapshot.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectEqualStrings("B", try snapshot.get(.{ .name = "other" }, "doc:a"));
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("A2", try txn.get(.{ .name = "docs" }, "doc:a"));
        try std.testing.expectError(error.NotFound, txn.get(.{ .name = "other" }, "doc:a"));
    }
}

fn expectBoundStoreConformance(runtime: *backend_erased.Store) !void {
    const caps = runtime.capabilities();
    try std.testing.expect(caps.cursors);
    try std.testing.expect(caps.ordered_ranges);
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

fn seedDurableState(runtime: *backend_erased.NamespaceStore) !void {
    {
        var txn = try runtime.beginWrite();
        try txn.put(.{}, "meta:lsn", "9");
        try txn.put(.{ .name = "docs" }, "doc:a", "A");
        try txn.put(.{ .name = "docs" }, "doc:b", "B");
        try txn.commit();
    }

    {
        var batch = try runtime.beginBatch();
        try batch.delete(.{ .name = "docs" }, "doc:b");
        try batch.put(.{ .name = "docs" }, "doc:c", "C");
        try batch.put(.{ .name = "other" }, "doc:z", "Z");
        try batch.commit();
    }
}

fn expectReopenedNamespaceState(runtime: *backend_erased.NamespaceStore) !void {
    var txn = try runtime.beginRead();
    defer txn.abort();
    try std.testing.expectEqualStrings("9", try txn.get(.{}, "meta:lsn"));
    try std.testing.expectEqualStrings("A", try txn.get(.{ .name = "docs" }, "doc:a"));
    try std.testing.expectError(error.NotFound, txn.get(.{ .name = "docs" }, "doc:b"));
    try std.testing.expectEqualStrings("C", try txn.get(.{ .name = "docs" }, "doc:c"));
    try std.testing.expectEqualStrings("Z", try txn.get(.{ .name = "other" }, "doc:z"));
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
}

var test_nonce: std.atomic.Value(u64) = .init(0);

fn nowNs() u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const nonce = test_nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-backend-conformance-{s}-{d}-{d}\x00", .{
        label,
        nowNs(),
        nonce,
    }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "backend conformance: memory backend" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    var ns_runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer ns_runtime.deinit();
    try expectNamespaceStoreConformance(&ns_runtime, false);

    var bound_runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer bound_runtime.deinit();
    try expectBoundStoreConformance(&bound_runtime);
}

test "backend conformance: lsm backend" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "lsm");
    defer cleanupTmp(path);

    var backend = try lsm_backend.Backend.open(std.testing.allocator, std.mem.span(path), .{
        .flush_threshold = 2,
        .compact_threshold_runs = 4,
    });
    defer backend.close();

    var ns_runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer ns_runtime.deinit();
    try expectNamespaceStoreConformance(&ns_runtime, false);

    var bound_runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer bound_runtime.deinit();
    try expectBoundStoreConformance(&bound_runtime);
}

test "backend conformance: lsm backend durable reopen" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "lsm-reopen");
    defer cleanupTmp(path);

    {
        var backend = try lsm_backend.Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 2,
            .compact_threshold_runs = 4,
        });
        defer backend.close();

        var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();
        try seedDurableState(&runtime);
    }

    {
        var backend = try lsm_backend.Backend.open(std.testing.allocator, std.mem.span(path), .{
            .flush_threshold = 2,
            .compact_threshold_runs = 4,
        });
        defer backend.close();

        var ns_runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
        defer ns_runtime.deinit();
        try expectReopenedNamespaceState(&ns_runtime);

        var bound_runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer bound_runtime.deinit();
        try expectReopenedBoundState(&bound_runtime);
    }
}

test "backend conformance: lmdb backend" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "lmdb");
    defer cleanupTmp(path);

    var backend = try lmdb_backend.Backend.open(std.testing.allocator, path, .{});
    defer backend.close();

    var ns_runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
    defer ns_runtime.deinit();
    try expectNamespaceStoreConformance(&ns_runtime, true);

    var bound_runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
    defer bound_runtime.deinit();
    try expectBoundStoreConformance(&bound_runtime);
}

test "backend conformance: lmdb backend durable reopen" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "lmdb-reopen");
    defer cleanupTmp(path);

    {
        var backend = try lmdb_backend.Backend.open(std.testing.allocator, path, .{});
        defer backend.close();

        var runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
        defer runtime.deinit();
        try seedDurableState(&runtime);
    }

    {
        var backend = try lmdb_backend.Backend.open(std.testing.allocator, path, .{});
        defer backend.close();

        var ns_runtime = try backend.runtimeNamespaceStore(std.testing.allocator);
        defer ns_runtime.deinit();
        try expectReopenedNamespaceState(&ns_runtime);

        var bound_runtime = try backend.runtimeStore(std.testing.allocator, .{ .name = "docs" });
        defer bound_runtime.deinit();
        try expectReopenedBoundState(&bound_runtime);
    }
}
