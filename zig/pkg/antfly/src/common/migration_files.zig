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
const fs = @import("fs_paths.zig");

pub fn writeAtomic(alloc: std.mem.Allocator, io: std.Io, path: []const u8, bytes: []const u8) !void {
    const temp = try std.fmt.allocPrint(alloc, "{s}.migration-tmp", .{path});
    defer alloc.free(temp);
    var file = try fs.createFilePortable(io, temp, .{ .truncate = true });
    defer file.close(io);
    try file.writePositionalAll(io, bytes, 0);
    try file.sync(io);
    try std.Io.Dir.rename(std.Io.Dir.cwd(), temp, std.Io.Dir.cwd(), path, io);
    try fs.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

/// Shared by standalone metadata and stopped-server tooling. Lock a stable
/// sibling inode because atomic catalog publication replaces the catalog file.
pub fn lockCatalog(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !std.Io.File {
    const lock_path = try std.fmt.allocPrint(alloc, "{s}.operator-lock", .{path});
    defer alloc.free(lock_path);
    if (std.fs.path.dirname(lock_path)) |parent| try fs.createDirPathPortable(io, parent);
    return std.Io.Dir.cwd().createFile(io, lock_path, .{
        .read = true,
        .truncate = false,
        .lock = .exclusive,
        .lock_nonblocking = true,
    }) catch |err| switch (err) {
        error.WouldBlock => error.VectorMigrationCatalogInUse,
        else => err,
    };
}
