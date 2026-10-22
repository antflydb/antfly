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

/// A staging directory that is cleaned only after this process created it
/// exclusively. Random names plus exclusive creation make collisions harmless.
pub const OwnedStagingDir = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    path: []u8,
    owned: bool = true,

    pub fn create(allocator: std.mem.Allocator, io: std.Io, destination: []const u8, label: []const u8) !OwnedStagingDir {
        const cwd = std.Io.Dir.cwd();
        if (std.fs.path.dirname(destination)) |parent| try cwd.createDirPath(io, parent);
        for (0..32) |_| {
            var nonce: u64 = undefined;
            try io.randomSecure(std.mem.asBytes(&nonce));
            const path = try std.fmt.allocPrint(allocator, "{s}.{s}-{x}", .{ destination, label, nonce });
            const created = createExclusive(cwd, io, path) catch |err| {
                allocator.free(path);
                return err;
            };
            if (!created) {
                allocator.free(path);
                continue;
            }
            return .{ .allocator = allocator, .io = io, .path = path };
        }
        return error.UnableToCreateStagingDirectory;
    }

    pub fn publish(self: *OwnedStagingDir, destination: []const u8) !void {
        try std.Io.Dir.cwd().renamePreserve(self.path, std.Io.Dir.cwd(), destination, self.io);
        self.owned = false;
    }

    pub fn deinit(self: *OwnedStagingDir) void {
        if (self.owned) std.Io.Dir.cwd().deleteTree(self.io, self.path) catch {};
        self.allocator.free(self.path);
        self.* = undefined;
    }
};

fn createExclusive(dir: std.Io.Dir, io: std.Io, path: []const u8) !bool {
    dir.createDir(io, path, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => return false,
        else => return err,
    };
    return true;
}

test "owned staging never adopts or deletes a collision" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root);
    const destination = try std.fs.path.join(allocator, &.{ root, "result" });
    defer allocator.free(destination);
    const collision = try std.fs.path.join(allocator, &.{ root, "collision" });
    defer allocator.free(collision);
    try std.Io.Dir.cwd().createDir(std.testing.io, collision, .default_dir);
    try std.testing.expect(!try createExclusive(std.Io.Dir.cwd(), std.testing.io, collision));
    try std.Io.Dir.cwd().access(std.testing.io, collision, .{});

    var staging = try OwnedStagingDir.create(allocator, std.testing.io, destination, "tmp");
    const created = try allocator.dupe(u8, staging.path);
    defer allocator.free(created);
    try std.Io.Dir.cwd().access(std.testing.io, created, .{});
    staging.deinit();
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, created, .{}));
    try std.Io.Dir.cwd().access(std.testing.io, collision, .{});
}
