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
const fs_paths = @import("../../common/fs_paths.zig");
const http_server = @import("../transport/http_server.zig");

pub const FileSnapshotStoreConfig = struct {
    root_dir: []const u8,
    max_snapshot_bytes: usize = 1 << 30,
};

pub const FileSnapshotStore = struct {
    alloc: std.mem.Allocator,
    cfg: FileSnapshotStoreConfig,
    io_impl: std.Io.Threaded,
    root_dir: []u8,

    pub fn init(alloc: std.mem.Allocator, cfg: FileSnapshotStoreConfig) !FileSnapshotStore {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = std.Io.Threaded.init(alloc, .{}),
            .root_dir = try alloc.dupe(u8, cfg.root_dir),
        };
    }

    pub fn deinit(self: *FileSnapshotStore) void {
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn store(self: *FileSnapshotStore) http_server.SnapshotStore {
        return .{
            .ptr = self,
            .vtable = &.{
                .put_snapshot = putSnapshot,
                .get_snapshot = getSnapshot,
            },
        };
    }

    fn putSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
        _ = alloc;
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const path = try self.snapshotPath(snapshot_id);
        defer self.alloc.free(path);

        const io_ctx = self.io_impl.io();
        try fs_paths.createDirPathPortable(io_ctx, self.root_dir);
        try std.Io.Dir.cwd().writeFile(io_ctx, .{
            .sub_path = path,
            .data = body,
        });
    }

    fn getSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
        const self: *FileSnapshotStore = @ptrCast(@alignCast(ptr));
        try validateSnapshotId(snapshot_id);
        const path = try self.snapshotPath(snapshot_id);
        defer self.alloc.free(path);

        return try std.Io.Dir.cwd().readFileAlloc(io(self), path, alloc, .limited(self.cfg.max_snapshot_bytes));
    }

    fn io(self: *FileSnapshotStore) std.Io {
        return self.io_impl.io();
    }

    fn snapshotPath(self: *const FileSnapshotStore, snapshot_id: []const u8) ![]u8 {
        return try std.fmt.allocPrint(self.alloc, "{s}/{s}.snap", .{ self.root_dir, snapshot_id });
    }

    fn validateSnapshotId(snapshot_id: []const u8) !void {
        if (snapshot_id.len == 0) return error.InvalidSnapshotId;
        if (std.mem.indexOf(u8, snapshot_id, "..") != null) return error.InvalidSnapshotId;
        for (snapshot_id) |c| {
            switch (c) {
                '/', '\\', 0 => return error.InvalidSnapshotId,
                else => {},
            }
        }
    }
};

test "file snapshot store persists snapshot bodies" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);

    var store = try FileSnapshotStore.init(std.testing.allocator, .{ .root_dir = root_dir });
    defer store.deinit();

    try store.store().putSnapshot(std.testing.allocator, "snap-1", "snapshot-body");
    const body = try store.store().getSnapshot(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(body);
    try std.testing.expectEqualStrings("snapshot-body", body);
}

test "file snapshot store rejects invalid snapshot ids" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/raft-snaps", .{tmp.sub_path});
    defer std.testing.allocator.free(root_dir);

    var store = try FileSnapshotStore.init(std.testing.allocator, .{ .root_dir = root_dir });
    defer store.deinit();

    try std.testing.expectError(error.InvalidSnapshotId, store.store().putSnapshot(std.testing.allocator, "../bad", "x"));
}
