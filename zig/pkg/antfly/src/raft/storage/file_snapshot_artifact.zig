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

const std = @import("std");
const raft_engine = @import("raft_engine");

pub const FileSnapshotArtifact = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []u8,
    size: u64,

    pub fn create(
        alloc: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
        size: u64,
    ) !raft_engine.runtime.storage_iface.SnapshotArtifact {
        const self = try alloc.create(FileSnapshotArtifact);
        errdefer alloc.destroy(self);
        self.* = .{
            .alloc = alloc,
            .io = io,
            .path = try alloc.dupe(u8, path),
            .size = size,
        };
        return .{
            .ptr = self,
            .vtable = &.{
                .len = len,
                .write_to = writeTo,
                .read_all = readAll,
                .deinit = deinit,
            },
        };
    }

    fn len(ptr: *anyopaque) u64 {
        const self: *FileSnapshotArtifact = @ptrCast(@alignCast(ptr));
        return self.size;
    }

    fn writeTo(ptr: *anyopaque, writer: *std.Io.Writer) !void {
        const self: *FileSnapshotArtifact = @ptrCast(@alignCast(ptr));
        var file = try std.Io.Dir.cwd().openFile(self.io, self.path, .{});
        defer file.close(self.io);
        var buffer: [64 * 1024]u8 = undefined;
        var offset: u64 = 0;
        while (offset < self.size) {
            const wanted: usize = @intCast(@min(buffer.len, self.size - offset));
            const read = try file.readPositionalAll(self.io, buffer[0..wanted], offset);
            if (read != wanted) return error.SnapshotArtifactTruncated;
            try writer.writeAll(buffer[0..read]);
            offset += read;
        }
    }

    fn readAll(ptr: *anyopaque, alloc: std.mem.Allocator) ![]u8 {
        const self: *FileSnapshotArtifact = @ptrCast(@alignCast(ptr));
        const limit = std.math.add(u64, self.size, 1) catch return error.SnapshotArtifactTooLarge;
        const data = try std.Io.Dir.cwd().readFileAlloc(self.io, self.path, alloc, .limited(limit));
        errdefer alloc.free(data);
        if (data.len != self.size) return error.SnapshotArtifactSizeMismatch;
        return data;
    }

    pub fn deinit(ptr: *anyopaque) void {
        const self: *FileSnapshotArtifact = @ptrCast(@alignCast(ptr));
        std.Io.Dir.cwd().deleteFile(self.io, self.path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => std.log.warn("raft snapshot spool cleanup failed path={s} error={s}", .{ self.path, @errorName(err) }),
        };
        self.alloc.free(self.path);
        const alloc = self.alloc;
        self.* = undefined;
        alloc.destroy(self);
    }
};
