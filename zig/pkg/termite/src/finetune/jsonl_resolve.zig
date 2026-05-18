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
const manifest_mod = @import("../io/manifest.zig");
const compat = @import("../io/compat.zig");

pub const ResolvedFiles = struct {
    arena: std.heap.ArenaAllocator,
    paths: [][]const u8,

    pub fn deinit(self: *ResolvedFiles) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn resolveJsonlFiles(allocator: std.mem.Allocator, path: []const u8, split: ?[]const u8) !ResolvedFiles {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    if (std.mem.trim(u8, path, " \t\r\n").len == 0) {
        return error.EmptyPath;
    }

    const stat = try compat.cwd().statFile(compat.io(), path, .{});
    if (stat.kind == .file) {
        if (std.mem.eql(u8, std.fs.path.basename(path), "manifest.json")) {
            const paths = try resolveFromManifest(arena_alloc, path, split);
            return .{ .arena = arena, .paths = paths };
        }
        const one = try arena_alloc.alloc([]const u8, 1);
        one[0] = try arena_alloc.dupe(u8, path);
        return .{ .arena = arena, .paths = one };
    }

    if (stat.kind != .directory) {
        return error.UnsupportedPathType;
    }

    const manifest_path = try std.fs.path.join(arena_alloc, &.{ path, "manifest.json" });
    if (compat.cwd().access(compat.io(), manifest_path, .{})) |_| {
        const paths = try resolveFromManifest(arena_alloc, manifest_path, split);
        return .{ .arena = arena, .paths = paths };
    } else |_| {
        const paths = try resolveFromDirectory(arena_alloc, path, split);
        return .{ .arena = arena, .paths = paths };
    }
}

fn resolveFromManifest(allocator: std.mem.Allocator, manifest_path: []const u8, split: ?[]const u8) ![][]const u8 {
    var loaded = try manifest_mod.load(allocator, manifest_path);
    defer loaded.deinit();

    const root = std.fs.path.dirname(manifest_path) orelse ".";
    var paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer paths.deinit(allocator);

    for (loaded.value.files) |entry| {
        if (entry.kind != .jsonl) continue;
        if (split) |want_split| {
            if (entry.split == null) continue;
            if (!std.mem.eql(u8, entry.split.?, want_split)) continue;
        }
        const full_path = try std.fs.path.join(allocator, &.{ root, entry.relative_path });
        try paths.append(allocator, full_path);
    }

    if (paths.items.len == 0) return error.NoJsonlFilesForSplit;
    std.sort.heap([]const u8, paths.items, {}, lessThanString);
    return try paths.toOwnedSlice(allocator);
}

fn resolveFromDirectory(allocator: std.mem.Allocator, dir_path: []const u8, split: ?[]const u8) ![][]const u8 {
    var dir = try compat.cwd().openDir(compat.io(), dir_path, .{ .iterate = true });
    defer dir.close(compat.io());

    var iter = dir.iterate();
    var paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer paths.deinit(allocator);

    while (try iter.next(compat.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".jsonl")) continue;
        if (split) |want_split| {
            const prefix = try std.fmt.allocPrint(allocator, "{s}-", .{want_split});
            defer allocator.free(prefix);
            if (!std.mem.startsWith(u8, entry.name, prefix)) continue;
        }
        const full_path = try std.fs.path.join(allocator, &.{ dir_path, entry.name });
        try paths.append(allocator, full_path);
    }

    if (paths.items.len == 0) return error.NoJsonlFilesForSplit;
    std.sort.heap([]const u8, paths.items, {}, lessThanString);
    return try paths.toOwnedSlice(allocator);
}

fn lessThanString(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}

test "resolve from manifest" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_json =
        \\{
        \\  "schema_version": 1,
        \\  "family": "lateinteraction",
        \\  "files": [
        \\    {
        \\      "relative_path": "train-00000.jsonl",
        \\      "kind": "jsonl",
        \\      "split": "train",
        \\      "records": 2,
        \\      "size": 10,
        \\      "digest": "sha256:a"
        \\    },
        \\    {
        \\      "relative_path": "val-00000.jsonl",
        \\      "kind": "jsonl",
        \\      "split": "val",
        \\      "records": 1,
        \\      "size": 5,
        \\      "digest": "sha256:b"
        \\    }
        \\  ]
        \\}
    ;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "manifest.json", .data = manifest_json });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "train-00000.jsonl", .data = "{}\n{}\n" });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "val-00000.jsonl", .data = "{}\n" });

    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(path);

    var resolved = try resolveJsonlFiles(allocator, path, "train");
    defer resolved.deinit();

    try std.testing.expectEqual(@as(usize, 1), resolved.paths.len);
    try std.testing.expect(std.mem.endsWith(u8, resolved.paths[0], "train-00000.jsonl"));
}
