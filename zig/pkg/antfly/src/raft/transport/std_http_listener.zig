// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
const common = @import("../../common/http/http_common.zig");
const listener_impl = @import("../../common/http/std_http_listener.zig");

pub const StdHttpListener = listener_impl.StdHttpListener;
pub const StdHttpListenerConfig = listener_impl.StdHttpListenerConfig;
pub const default_max_request_bytes = listener_impl.default_max_request_bytes;

test "std http listener and executor round-trip raft batch route" {
    const raft_engine = @import("raft_engine");
    const http_driver = @import("../../raft/transport/http_driver.zig");
    const http_server = @import("../../raft/transport/http_server.zig");
    const std_http_executor = @import("../../common/http/std_http_executor.zig");

    const Handler = struct {
        seen: usize = 0,

        fn iface(self: *@This()) http_server.BatchHandler {
            return .{
                .ptr = self,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += batch.groups.len;
        }
    };

    var handler = Handler{};
    var app = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        handler.iface(),
        null,
        null,
    );
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor: std_http_executor.StdHttpExecutor = undefined;
    executor.initInPlace(std.testing.allocator, .{});
    defer executor.deinit();
    var driver = http_driver.HttpFrameDriver.init(std.testing.allocator, .{}, executor.executor(), executor.io_impl.io());

    const msg = raft_engine.core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 3,
    };
    const batch = raft_engine.runtime.transport_iface.PeerBatch{
        .peer_id = 2,
        .groups = (&[_]raft_engine.runtime.transport_iface.GroupMessageBatch{
            .{
                .group_id = 55,
                .messages = (&[_]raft_engine.core.Message{msg})[0..],
            },
        })[0..],
    };
    const frame = try raft_engine.runtime.BinaryCodec.codec().encodePeerBatch(std.testing.allocator, batch);
    defer raft_engine.runtime.BinaryCodec.codec().freeFrame(std.testing.allocator, frame);

    try driver.sendBatch(.{
        .peer_id = 2,
        .base_uri = base_uri,
        .body = frame.bytes,
        .content_type = frame.media_type,
    });
    try std.testing.expectEqual(@as(usize, 1), handler.seen);
}

test "std http listener and executor round-trip snapshot routes" {
    const raft_engine = @import("raft_engine");
    const http_server = @import("../../raft/transport/http_server.zig");
    const http_snapshot = @import("../../raft/transport/http_snapshot.zig");
    const std_http_executor = @import("../../common/http/std_http_executor.zig");
    const routes = @import("../../raft/transport/routes.zig");

    const Store = struct {
        body: ?[]u8 = null,

        fn iface(self: *@This()) http_server.SnapshotStore {
            return .{
                .ptr = self,
                .vtable = &.{
                    .put_snapshot = putSnapshot,
                    .get_snapshot = getSnapshot,
                },
            };
        }

        fn putSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body) |existing| alloc.free(existing);
            self.body = try alloc.dupe(u8, body);
        }

        fn getSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try alloc.dupe(u8, self.body.?);
        }
    };

    const Noop = struct {
        fn iface(_: *@This()) http_server.BatchHandler {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(_: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            _ = batch;
        }
    };

    const Receiver = struct {
        seen: usize = 0,
        index: u64 = 0,

        fn iface(self: *@This()) raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .receive_snapshot = receiveSnapshot,
                },
            };
        }

        fn receiveSnapshot(
            ptr: *anyopaque,
            req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            snapshot: raft_engine.core.types.Snapshot,
        ) !void {
            _ = req;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = snapshot;
            defer owned.deinit(std.testing.allocator);
            self.seen += 1;
            self.index = snapshot.metadata.index;
        }
    };

    var store = Store{};
    defer if (store.body) |body| std.testing.allocator.free(body);
    var noop = Noop{};
    var app = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        noop.iface(),
        store.iface(),
        null,
    );
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor: std_http_executor.StdHttpExecutor = undefined;
    executor.initInPlace(std.testing.allocator, .{});
    defer executor.deinit();
    var transport = try http_snapshot.HttpSnapshotTransport.init(
        std.testing.allocator,
        .{ .root_dir = "/tmp" },
        executor.executor(),
        null,
    );
    defer transport.deinit();

    const upload_path = try routes.Routes.snapshotUploadPath(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(upload_path);
    const upload_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_uri, upload_path });
    defer std.testing.allocator.free(upload_uri);

    const fetch_path = try routes.Routes.snapshotFetchPath(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(fetch_path);
    const fetch_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_uri, fetch_path });
    defer std.testing.allocator.free(fetch_uri);

    var voters = [_]u64{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.alloc(u8, 16 * 1024 + 37);
    defer std.testing.allocator.free(snapshot_bytes);
    for (snapshot_bytes, 0..) |*byte, i| byte.* = @intCast('a' + (i % 26));

    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .to = 2,
        .snapshot = .{
            .metadata = .{
                .index = 12,
                .term = 4,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "snap-1", .uri = upload_uri },
    });

    var receiver = Receiver{};
    try transport.transport().fetchSnapshot(.{
        .group_id = 91,
        .from = 2,
        .locator = .{ .snapshot_id = "snap-1", .uri = fetch_uri },
    }, receiver.iface());
    try std.testing.expectEqual(@as(usize, 1), receiver.seen);
    try std.testing.expectEqual(@as(u64, 12), receiver.index);
}
