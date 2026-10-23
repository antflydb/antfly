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
const raft_engine = @import("raft_engine");

pub const FileSnapshotArtifact = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []u8,
    size: u64,
    application_manifest: []u8,

    pub fn create(
        alloc: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
        size: u64,
    ) !raft_engine.runtime.storage_iface.SnapshotArtifact {
        return createWithApplicationManifest(alloc, io, path, size, "");
    }

    pub fn createWithApplicationManifest(alloc: std.mem.Allocator, io: std.Io, path: []const u8, size: u64, manifest: []const u8) !raft_engine.runtime.storage_iface.SnapshotArtifact {
        if (manifest.len > raft_engine.runtime.storage_iface.SnapshotArtifact.max_application_manifest_bytes) return error.SnapshotPayloadTooLarge;
        const self = try alloc.create(FileSnapshotArtifact);
        errdefer alloc.destroy(self);
        const owned_path = try alloc.dupe(u8, path);
        errdefer alloc.free(owned_path);
        self.* = .{
            .alloc = alloc,
            .io = io,
            .path = owned_path,
            .size = size,
            .application_manifest = try alloc.dupe(u8, manifest),
        };
        return .{
            .ptr = self,
            .vtable = &.{
                .len = len,
                .write_to = writeTo,
                .read_all = readAll,
                .application_manifest = applicationManifest,
                .deinit = deinit,
            },
        };
    }

    fn applicationManifest(ptr: *anyopaque) []const u8 {
        const self: *FileSnapshotArtifact = @ptrCast(@alignCast(ptr));
        return self.application_manifest;
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
        self.alloc.free(self.application_manifest);
        const alloc = self.alloc;
        self.* = undefined;
        alloc.destroy(self);
    }
};
