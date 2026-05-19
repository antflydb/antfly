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
const raft_engine = @import("raft_engine");
const common = @import("http_common.zig");
const routes = @import("routes.zig");

pub const HttpServerConfig = struct {
    max_request_bytes: usize = 1 << 20,
};

pub const BatchHandler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        handle_peer_batch: *const fn (ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) anyerror!void,
    };

    pub fn handlePeerBatch(self: BatchHandler, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
        return try self.vtable.handle_peer_batch(self.ptr, batch);
    }
};

pub const SnapshotStore = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        put_snapshot: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) anyerror!void,
        get_snapshot: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) anyerror![]u8,
    };

    pub fn putSnapshot(self: SnapshotStore, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
        return try self.vtable.put_snapshot(self.ptr, alloc, snapshot_id, body);
    }

    pub fn getSnapshot(self: SnapshotStore, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
        return try self.vtable.get_snapshot(self.ptr, alloc, snapshot_id);
    }
};

pub const HttpServer = struct {
    alloc: std.mem.Allocator,
    cfg: HttpServerConfig,
    codec: raft_engine.runtime.MessageCodec,
    batch_handler: BatchHandler,
    snapshot_store: ?SnapshotStore = null,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: HttpServerConfig,
        codec: raft_engine.runtime.MessageCodec,
        batch_handler: BatchHandler,
        snapshot_store: ?SnapshotStore,
    ) HttpServer {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .codec = codec,
            .batch_handler = batch_handler,
            .snapshot_store = snapshot_store,
        };
    }

    pub fn start(self: *HttpServer) !void {
        _ = self;
    }

    pub fn executor(self: *HttpServer) common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn handle(self: *HttpServer, req: common.HttpRequest) !common.HttpResponse {
        if (std.mem.eql(u8, req.uri, routes.Routes.health) and req.method == .GET) {
            return .{
                .status = 200,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "ok"),
            };
        }
        if (std.mem.eql(u8, req.uri, routes.Routes.raft_batch) and req.method == .POST) {
            if (req.body.len > self.cfg.max_request_bytes) return error.RequestTooLarge;
            const decoded = try self.codec.decodeFrame(self.alloc, .{
                .bytes = @constCast(req.body),
                .media_type = req.content_type orelse "application/octet-stream",
            });
            defer self.codec.freeDecoded(self.alloc, decoded);
            switch (decoded) {
                .raft_peer_batch => |batch| try self.batch_handler.handlePeerBatch(batch),
                else => return error.UnsupportedFrame,
            }
            return .{
                .status = 202,
                .content_type = try self.alloc.dupe(u8, "text/plain"),
                .body = try self.alloc.dupe(u8, "accepted"),
            };
        }
        if (req.method == .POST) {
            if (routes.Routes.matchSnapshotUpload(req.uri)) |snapshot_id| {
                const store = self.snapshot_store orelse return error.MissingSnapshotStore;
                try store.putSnapshot(self.alloc, snapshot_id, req.body);
                return .{
                    .status = 201,
                    .content_type = try self.alloc.dupe(u8, "text/plain"),
                    .body = try self.alloc.dupe(u8, "stored"),
                };
            }
        }
        if (req.method == .GET) {
            if (routes.Routes.matchSnapshotFetch(req.uri)) |snapshot_id| {
                const store = self.snapshot_store orelse return error.MissingSnapshotStore;
                const body = try store.getSnapshot(self.alloc, snapshot_id);
                return .{
                    .status = 200,
                    .content_type = try self.alloc.dupe(u8, "application/x-antflydb-raft-snapshot"),
                    .body = body,
                };
            }
        }
        return .{
            .status = 404,
            .content_type = try self.alloc.dupe(u8, "text/plain"),
            .body = try self.alloc.dupe(u8, "not found"),
        };
    }

    fn execute(ptr: *anyopaque, _: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const self: *HttpServer = @ptrCast(@alignCast(ptr));
        return try self.handle(req);
    }
};

test "http server module compiles" {
    _ = HttpServerConfig;
    _ = BatchHandler;
    _ = SnapshotStore;
    _ = HttpServer;
}

test "http server exposes request executor" {
    const Handler = struct {
        fn iface(_: *@This()) BatchHandler {
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

    var handler = Handler{};
    var server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), handler.iface(), null);
    const executor = server.executor();
    var resp = try executor.execute(std.testing.allocator, .{
        .method = .GET,
        .uri = routes.Routes.health,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
}

test "http server decodes raft batch requests and dispatches them" {
    const Handler = struct {
        seen: usize = 0,

        fn iface(self: *@This()) BatchHandler {
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
    var server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), handler.iface(), null);

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

    var resp = try server.handle(.{
        .method = .POST,
        .uri = routes.Routes.raft_batch,
        .content_type = frame.media_type,
        .body = frame.bytes,
    });
    defer resp.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 202), resp.status);
    try std.testing.expectEqual(@as(usize, 1), handler.seen);
}

test "http server stores and fetches snapshot bodies by route" {
    const Store = struct {
        body: ?[]u8 = null,

        fn iface(self: *@This()) SnapshotStore {
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
        fn iface(_: *@This()) BatchHandler {
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

    var store = Store{};
    defer if (store.body) |body| std.testing.allocator.free(body);
    var noop = Noop{};
    var server = HttpServer.init(std.testing.allocator, .{}, raft_engine.runtime.BinaryCodec.codec(), noop.iface(), store.iface());

    const upload_path = try routes.Routes.snapshotUploadPath(std.testing.allocator, "snap-7");
    defer std.testing.allocator.free(upload_path);
    var upload = try server.handle(.{
        .method = .POST,
        .uri = upload_path,
        .body = "snapshot-body",
    });
    defer upload.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 201), upload.status);

    const fetch_path = try routes.Routes.snapshotFetchPath(std.testing.allocator, "snap-7");
    defer std.testing.allocator.free(fetch_path);
    var fetch = try server.handle(.{
        .method = .GET,
        .uri = fetch_path,
    });
    defer fetch.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), fetch.status);
    try std.testing.expectEqualStrings("snapshot-body", fetch.body);
}
