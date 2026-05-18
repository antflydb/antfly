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
const wal_store = @import("store.zig");
const object_store = @import("object_store.zig");

pub const RemoteStore = struct {
    object: object_store.ObjectStore,

    pub fn init(alloc: std.mem.Allocator, uri: []const u8) !RemoteStore {
        return .{ .object = try object_store.ObjectStore.initRemoteUri(alloc, uri) };
    }

    pub fn deinit(self: *RemoteStore) void {
        self.object.deinit();
        self.* = undefined;
    }

    pub fn walStore(self: *RemoteStore) wal_store.WalStore {
        return self.object.walStore();
    }
};

test "remote wal store opens shared fs backend from file uri" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "wal-remote");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    var remote = try RemoteStore.init(std.testing.allocator, uri);
    var store = remote.walStore();
    defer store.deinit();

    _ = try store.append("docs", 10, "payload");
    const records = try store.readFromAlloc("docs", 1);
    defer @import("mod.zig").freeRecords(std.testing.allocator, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
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
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-{s}-{d}-{d}\x00", .{ label, nowNs(), nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
