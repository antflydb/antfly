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
const fs = @import("antfly_runtime_fs").fs_paths;

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

// The released stopped-server operator only understands JSON catalogs. New
// standalone instances migrate authority into this sibling durable store;
// changing their obsolete JSON file would leave native and catalog ownership
// inconsistent. Reject before admitting a job or touching its source root.
pub fn requireJsonCatalogAuthority(alloc: std.mem.Allocator, io: std.Io, catalog_path: []const u8) !void {
    const authority_path = try std.fs.path.join(alloc, &.{ std.fs.path.dirname(catalog_path) orelse ".", "local-state" });
    defer alloc.free(authority_path);
    std.Io.Dir.cwd().access(io, authority_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    return error.VectorMigrationAuthoritativeCatalogUnsupported;
}

test "offline migration rejects authoritative metadata before changing legacy catalog" {
    const alloc = std.testing.allocator;
    const io = std.Options.debug_io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const catalog_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/catalog.json", .{tmp.sub_path});
    defer alloc.free(catalog_path);
    const original = "{\"epoch\":7,\"tables\":[],\"ranges\":[]}";
    try tmp.dir.writeFile(io, .{ .sub_path = "catalog.json", .data = original });
    try requireJsonCatalogAuthority(alloc, io, catalog_path);
    try tmp.dir.createDir(io, "local-state", .default_dir);
    try std.testing.expectError(error.VectorMigrationAuthoritativeCatalogUnsupported, requireJsonCatalogAuthority(alloc, io, catalog_path));
    const unchanged = try tmp.dir.readFileAlloc(io, "catalog.json", alloc, .limited(1024));
    defer alloc.free(unchanged);
    try std.testing.expectEqualStrings(original, unchanged);
}
