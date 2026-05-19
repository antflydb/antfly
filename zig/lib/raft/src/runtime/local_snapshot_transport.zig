// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const core = @import("../core/mod.zig");
const snapshot_transport_iface = @import("snapshot_transport_iface.zig");

pub const LocalSnapshotTransport = struct {
    alloc: std.mem.Allocator,
    root_dir: []const u8,

    pub fn init(alloc: std.mem.Allocator, root_dir: []const u8) !LocalSnapshotTransport {
        var io: std.Io.Threaded = .init(alloc, .{});
        defer io.deinit();
        try std.Io.Dir.cwd().createDirPath(io.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
        };
    }

    pub fn deinit(self: *LocalSnapshotTransport) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn transport(self: *LocalSnapshotTransport) snapshot_transport_iface.SnapshotTransport {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_snapshot = sendSnapshot,
                .fetch_snapshot = fetchSnapshot,
                .cancel_snapshot = cancelSnapshot,
            },
        };
    }

    fn sendSnapshot(ptr: *anyopaque, req: snapshot_transport_iface.SnapshotSendRequest) !void {
        const self: *LocalSnapshotTransport = @ptrCast(@alignCast(ptr));
        const snapshot_key = if (req.locator) |locator|
            try self.alloc.dupe(u8, locator.snapshot_id)
        else
            try std.fmt.allocPrint(self.alloc, "{d}", .{req.to});
        defer self.alloc.free(snapshot_key);
        var io: std.Io.Threaded = .init(self.alloc, .{});
        defer io.deinit();
        const data_path = try std.fmt.allocPrint(self.alloc, "{s}/{d}-{s}.bin", .{ self.root_dir, req.group_id, snapshot_key });
        defer self.alloc.free(data_path);
        const meta_path = try std.fmt.allocPrint(self.alloc, "{s}/{d}-{s}.meta", .{ self.root_dir, req.group_id, snapshot_key });
        defer self.alloc.free(meta_path);

        var file = try std.Io.Dir.cwd().createFile(io.io(), data_path, .{ .truncate = true });
        defer file.close(io.io());
        var file_buffer: [1024]u8 = undefined;
        var file_writer = file.writer(io.io(), &file_buffer);
        try file_writer.interface.writeAll(req.snapshot.data);
        try file_writer.interface.flush();

        const meta_bytes = try encodeSnapshotMetadata(self.alloc, req.snapshot.metadata);
        defer self.alloc.free(meta_bytes);
        var meta_file = try std.Io.Dir.cwd().createFile(io.io(), meta_path, .{ .truncate = true });
        defer meta_file.close(io.io());
        var meta_buffer: [512]u8 = undefined;
        var meta_writer = meta_file.writer(io.io(), &meta_buffer);
        try meta_writer.interface.writeAll(meta_bytes);
        try meta_writer.interface.flush();
    }

    fn fetchSnapshot(
        ptr: *anyopaque,
        req: snapshot_transport_iface.SnapshotFetchRequest,
        receiver: snapshot_transport_iface.SnapshotReceiver,
    ) !void {
        const self: *LocalSnapshotTransport = @ptrCast(@alignCast(ptr));
        var io: std.Io.Threaded = .init(self.alloc, .{});
        defer io.deinit();
        const data_path = try std.fmt.allocPrint(self.alloc, "{s}/{d}-{s}.bin", .{ self.root_dir, req.group_id, req.locator.snapshot_id });
        defer self.alloc.free(data_path);
        const meta_path = try std.fmt.allocPrint(self.alloc, "{s}/{d}-{s}.meta", .{ self.root_dir, req.group_id, req.locator.snapshot_id });
        defer self.alloc.free(meta_path);
        const data = try std.Io.Dir.cwd().readFileAlloc(io.io(), data_path, self.alloc, .limited(1024 * 1024));
        const meta_bytes = try std.Io.Dir.cwd().readFileAlloc(io.io(), meta_path, self.alloc, .limited(64 * 1024));
        defer self.alloc.free(meta_bytes);
        const metadata = try decodeSnapshotMetadata(self.alloc, meta_bytes);
        var snapshot: core.types.Snapshot = .{
            .metadata = metadata,
            .data = data,
        };
        errdefer snapshot.deinit(self.alloc);
        try receiver.receiveSnapshot(.{
            .group_id = req.group_id,
            .from = req.from,
            .term = req.term,
            .locator = req.locator,
        }, snapshot);
    }

    fn cancelSnapshot(ptr: *anyopaque, group_id: core.types.GroupId, snapshot_id: []const u8) !void {
        _ = ptr;
        _ = group_id;
        _ = snapshot_id;
    }

    fn encodeSnapshotMetadata(alloc: std.mem.Allocator, metadata: core.types.SnapshotMetadata) ![]u8 {
        const conf = metadata.conf_state;
        const total_len: usize =
            @sizeOf(u64) + // index
            @sizeOf(u64) + // term
            @sizeOf(u8) + // auto_leave
            @sizeOf(u32) * 4 +
            conf.voters.len * @sizeOf(core.types.NodeId) +
            conf.voters_outgoing.len * @sizeOf(core.types.NodeId) +
            conf.learners.len * @sizeOf(core.types.NodeId) +
            conf.learners_next.len * @sizeOf(core.types.NodeId);

        const out = try alloc.alloc(u8, total_len);
        var cursor: usize = 0;
        writeIntAt(u64, out, &cursor, metadata.index);
        writeIntAt(u64, out, &cursor, metadata.term);
        out[cursor] = @intFromBool(conf.auto_leave);
        cursor += 1;
        writeNodeList(out, &cursor, conf.voters);
        writeNodeList(out, &cursor, conf.voters_outgoing);
        writeNodeList(out, &cursor, conf.learners);
        writeNodeList(out, &cursor, conf.learners_next);
        return out;
    }

    fn decodeSnapshotMetadata(alloc: std.mem.Allocator, data: []const u8) !core.types.SnapshotMetadata {
        var cursor: usize = 0;
        const index = readIntAt(u64, data, &cursor);
        const term = readIntAt(u64, data, &cursor);
        if (cursor >= data.len) return error.InvalidSnapshotMetadataEncoding;
        const auto_leave = data[cursor] != 0;
        cursor += 1;

        return .{
            .index = index,
            .term = term,
            .conf_state = .{
                .voters = try readNodeList(alloc, data, &cursor),
                .voters_outgoing = try readNodeList(alloc, data, &cursor),
                .learners = try readNodeList(alloc, data, &cursor),
                .learners_next = try readNodeList(alloc, data, &cursor),
                .auto_leave = auto_leave,
            },
        };
    }

    fn writeNodeList(out: []u8, cursor: *usize, nodes: []const core.types.NodeId) void {
        writeIntAt(u32, out, cursor, @as(u32, @intCast(nodes.len)));
        for (nodes) |node| writeIntAt(core.types.NodeId, out, cursor, node);
    }

    fn readNodeList(alloc: std.mem.Allocator, data: []const u8, cursor: *usize) ![]core.types.NodeId {
        const len: usize = @intCast(readIntAt(u32, data, cursor));
        const out = try alloc.alloc(core.types.NodeId, len);
        errdefer alloc.free(out);
        for (out) |*node| node.* = readIntAt(core.types.NodeId, data, cursor);
        return out;
    }

    fn writeIntAt(comptime T: type, out: []u8, cursor: *usize, value: T) void {
        std.mem.writeInt(T, out[cursor.* .. cursor.* + @sizeOf(T)][0..@sizeOf(T)], value, .little);
        cursor.* += @sizeOf(T);
    }

    fn readIntAt(comptime T: type, data: []const u8, cursor: *usize) T {
        const value = std.mem.readInt(T, data[cursor.* .. cursor.* + @sizeOf(T)][0..@sizeOf(T)], .little);
        cursor.* += @sizeOf(T);
        return value;
    }
};

test "local snapshot transport sends and fetches snapshot bytes" {
    const Receiver = struct {
        bytes: []u8 = &.{},

        fn iface(self: *@This()) snapshot_transport_iface.SnapshotReceiver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .receive_snapshot = receiveSnapshot,
                },
            };
        }

        fn receiveSnapshot(
            ptr: *anyopaque,
            req: snapshot_transport_iface.SnapshotFetchRequest,
            snapshot: core.types.Snapshot,
        ) !void {
            _ = req;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            defer {
                var owned_snapshot = snapshot;
                owned_snapshot.deinit(std.testing.allocator);
            }
            self.bytes = try std.testing.allocator.dupe(u8, snapshot.data);
        }
    };

    const root_dir = "/tmp/antflydb-raft-local-snapshot-transport";
    var transport = try LocalSnapshotTransport.init(std.testing.allocator, root_dir);
    defer transport.deinit();
    const snapshot_bytes = try std.testing.allocator.dupe(u8, "snap-bytes");
    defer std.testing.allocator.free(snapshot_bytes);

    try transport.transport().sendSnapshot(.{
        .group_id = 3,
        .to = 2,
        .term = 7,
        .snapshot = .{
            .metadata = .{},
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "ignored" },
    });

    var receiver = Receiver{};
    defer if (receiver.bytes.len > 0) std.testing.allocator.free(receiver.bytes);
    try transport.transport().fetchSnapshot(.{
        .group_id = 3,
        .from = 2,
        .term = 7,
        .locator = .{ .snapshot_id = "ignored" },
    }, receiver.iface());

    try std.testing.expectEqualStrings("snap-bytes", receiver.bytes);
}
