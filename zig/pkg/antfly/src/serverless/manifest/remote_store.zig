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
const manifest_store = @import("store.zig");
const object_store = @import("object_store.zig");
const manifest_types = @import("types.zig");

pub const RemoteStore = struct {
    object: object_store.ObjectStore,

    pub fn init(alloc: std.mem.Allocator, uri: []const u8) !RemoteStore {
        return .{ .object = try object_store.ObjectStore.initRemoteUri(alloc, uri) };
    }

    pub fn deinit(self: *RemoteStore) void {
        self.object.deinit();
        self.* = undefined;
    }

    pub fn manifestStore(self: *RemoteStore) manifest_store.ManifestStore {
        return self.object.manifestStore();
    }
};

test "remote manifest store opens shared fs backend from file uri" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf, "manifest-remote");
    defer cleanupTmp(path);

    const uri = try std.fmt.allocPrint(std.testing.allocator, "file://{s}", .{std.mem.span(path)});
    defer std.testing.allocator.free(uri);

    var remote = try RemoteStore.init(std.testing.allocator, uri);
    var store = remote.manifestStore();
    defer store.deinit();

    var manifest = manifest_types.Manifest{
        .namespace = try std.testing.allocator.dupe(u8, "docs"),
        .version = 1,
        .built_at_ns = 10,
        .wal_start_lsn = 1,
        .wal_end_lsn = 1,
        .stats = .{},
        .artifacts = try std.testing.allocator.alloc(manifest_types.ArtifactRef, 0),
    };
    defer manifest.deinit(std.testing.allocator);

    try store.put(manifest);
    try store.setHead("docs", 1);
    try std.testing.expectEqual(@as(u64, 1), try store.getHead("docs"));
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
