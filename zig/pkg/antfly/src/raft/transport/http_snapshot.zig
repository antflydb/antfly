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
const http_server = @import("http_server.zig");
const routes = @import("routes.zig");

pub const HttpSnapshotConfig = struct {
    root_dir: []const u8,
    chunk_size: usize = 1 << 20,
};

pub const SnapshotTargetResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_upload_uri: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            node_id: u64,
            snapshot_id: []const u8,
        ) anyerror![]u8,
    };

    pub fn resolveUploadUri(
        self: SnapshotTargetResolver,
        alloc: std.mem.Allocator,
        group_id: u64,
        node_id: u64,
        snapshot_id: []const u8,
    ) ![]u8 {
        return try self.vtable.resolve_upload_uri(self.ptr, alloc, group_id, node_id, snapshot_id);
    }
};

pub const SnapshotFetch = struct {
    group_id: u64,
    snapshot_id: []const u8,
    uri: []const u8,
};

pub const HttpSnapshotTransport = struct {
    alloc: std.mem.Allocator,
    cfg: HttpSnapshotConfig,
    executor: common.RequestExecutor,
    resolver: ?SnapshotTargetResolver = null,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: HttpSnapshotConfig,
        executor: common.RequestExecutor,
        resolver: ?SnapshotTargetResolver,
    ) HttpSnapshotTransport {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .executor = executor,
            .resolver = resolver,
        };
    }

    pub fn transport(self: *HttpSnapshotTransport) raft_engine.runtime.SnapshotTransport {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_snapshot = sendSnapshot,
                .fetch_snapshot = fetchSnapshot,
            },
        };
    }

    pub fn fetch(self: *HttpSnapshotTransport, req: SnapshotFetch) ![]u8 {
        var resp = try self.executor.execute(self.alloc, .{
            .method = .GET,
            .uri = req.uri,
        });
        errdefer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
        const body = resp.body;
        resp.body = &.{};
        resp.deinit(self.alloc);
        return body;
    }

    fn sendSnapshot(ptr: *anyopaque, req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest) !void {
        const self: *HttpSnapshotTransport = @ptrCast(@alignCast(ptr));
        const uri = try self.resolveUploadUri(req);
        defer self.alloc.free(uri);

        const body = try encodeSnapshotEnvelope(self.alloc, req.snapshot);
        defer self.alloc.free(body);

        var resp = try self.executor.execute(self.alloc, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/x-antflydb-raft-snapshot",
            .body = body,
        });
        defer resp.deinit(self.alloc);
        if (resp.status < 200 or resp.status >= 300) return error.UnexpectedHttpStatus;
    }

    fn fetchSnapshot(
        ptr: *anyopaque,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
        receiver: raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver,
    ) !void {
        const self: *HttpSnapshotTransport = @ptrCast(@alignCast(ptr));
        const bytes = try self.fetch(.{
            .group_id = req.group_id,
            .snapshot_id = req.locator.snapshot_id,
            .uri = req.locator.uri,
        });
        defer self.alloc.free(bytes);
        const snapshot = try decodeSnapshotEnvelope(self.alloc, bytes);
        try receiver.receiveSnapshot(req, snapshot);
    }

    fn resolveUploadUri(
        self: *HttpSnapshotTransport,
        req: raft_engine.runtime.snapshot_transport_iface.SnapshotSendRequest,
    ) ![]u8 {
        if (req.locator) |locator| {
            if (locator.uri.len > 0) return try self.alloc.dupe(u8, locator.uri);
            if (self.resolver) |resolver| {
                return try resolver.resolveUploadUri(self.alloc, req.group_id, req.to, locator.snapshot_id);
            }
        }
        return error.MissingSnapshotUploadUri;
    }

    fn encodeSnapshotEnvelope(alloc: std.mem.Allocator, snapshot: raft_engine.core.types.Snapshot) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        defer out.deinit(alloc);

        try appendInt(u64, alloc, &out, snapshot.metadata.index);
        try appendInt(u64, alloc, &out, snapshot.metadata.term);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.voters);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.voters_outgoing);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.learners);
        try encodeNodeList(alloc, &out, snapshot.metadata.conf_state.learners_next);
        try out.append(alloc, @intFromBool(snapshot.metadata.conf_state.auto_leave));
        try appendBytes(alloc, &out, snapshot.data);
        return try out.toOwnedSlice(alloc);
    }

    fn decodeSnapshotEnvelope(alloc: std.mem.Allocator, bytes: []const u8) !raft_engine.core.types.Snapshot {
        var cursor: usize = 0;
        const index = try readInt(u64, bytes, &cursor);
        const term = try readInt(u64, bytes, &cursor);
        var conf_state: raft_engine.core.types.ConfState = .{};
        errdefer conf_state.deinit(alloc);
        conf_state.voters = try decodeNodeList(alloc, bytes, &cursor);
        conf_state.voters_outgoing = try decodeNodeList(alloc, bytes, &cursor);
        conf_state.learners = try decodeNodeList(alloc, bytes, &cursor);
        conf_state.learners_next = try decodeNodeList(alloc, bytes, &cursor);
        conf_state.auto_leave = try readBool(bytes, &cursor);
        const data = try readBytes(alloc, bytes, &cursor);
        return .{
            .metadata = .{
                .index = index,
                .term = term,
                .conf_state = conf_state,
            },
            .data = data,
        };
    }

    fn appendInt(comptime T: type, alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: T) !void {
        var buf: [@sizeOf(T)]u8 = undefined;
        std.mem.writeInt(T, &buf, value, .little);
        try out.appendSlice(alloc, &buf);
    }

    fn appendBytes(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), bytes: []const u8) !void {
        try appendInt(u32, alloc, out, @intCast(bytes.len));
        try out.appendSlice(alloc, bytes);
    }

    fn encodeNodeList(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), nodes: []const u64) !void {
        try appendInt(u32, alloc, out, @intCast(nodes.len));
        for (nodes) |node| try appendInt(u64, alloc, out, node);
    }

    fn readInt(comptime T: type, data: []const u8, cursor: *usize) !T {
        if (cursor.* + @sizeOf(T) > data.len) return error.InvalidSnapshotEnvelope;
        var buf: [@sizeOf(T)]u8 = undefined;
        @memcpy(&buf, data[cursor.* .. cursor.* + @sizeOf(T)]);
        const value = std.mem.readInt(T, &buf, .little);
        cursor.* += @sizeOf(T);
        return value;
    }

    fn readBool(data: []const u8, cursor: *usize) !bool {
        if (cursor.* >= data.len) return error.InvalidSnapshotEnvelope;
        const value = data[cursor.*] != 0;
        cursor.* += 1;
        return value;
    }

    fn readBytes(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) ![]u8 {
        const len = try readInt(u32, data, cursor);
        if (cursor.* + len > data.len) return error.InvalidSnapshotEnvelope;
        defer cursor.* += len;
        return try alloc.dupe(u8, data[cursor.* .. cursor.* + len]);
    }

    fn decodeNodeList(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) ![]u64 {
        const len = try readInt(u32, data, cursor);
        const out = try alloc.alloc(u64, len);
        errdefer alloc.free(out);
        for (out) |*node| node.* = try readInt(u64, data, cursor);
        return out;
    }
};

test "http snapshot transport module compiles" {
    _ = HttpSnapshotConfig;
    _ = SnapshotTargetResolver;
    _ = SnapshotFetch;
    _ = HttpSnapshotTransport;
}

test "http snapshot transport posts and fetches serialized snapshots" {
    const RecordingExecutor = struct {
        server: *http_server.HttpServer,

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            return try self.server.handle(req);
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

    var store_impl = Store{};
    defer if (store_impl.body) |body| std.testing.allocator.free(body);
    var noop = Noop{};
    var server = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        noop.iface(),
        store_impl.iface(),
    );
    var executor = RecordingExecutor{ .server = &server };
    var transport = HttpSnapshotTransport.init(std.testing.allocator, .{
        .root_dir = "/tmp",
    }, executor.iface(), null);

    var voters = [_]u64{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.dupe(u8, "snapshot-bytes");
    defer std.testing.allocator.free(snapshot_bytes);

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
        .locator = .{ .snapshot_id = "snap-1", .uri = "/raft/v1/snapshot/upload/snap-1" },
    });

    var receiver = Receiver{};
    try transport.transport().fetchSnapshot(.{
        .group_id = 91,
        .from = 2,
        .locator = .{ .snapshot_id = "snap-1", .uri = "/raft/v1/snapshot/fetch/snap-1" },
    }, receiver.iface());
    try std.testing.expectEqual(@as(usize, 1), receiver.seen);
    try std.testing.expectEqual(@as(u64, 12), receiver.index);
}
