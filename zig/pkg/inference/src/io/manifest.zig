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
const compat = @import("compat.zig");

pub const ExportKind = enum {
    jsonl,
    asset,
};

pub const FileEntry = struct {
    relative_path: []const u8,
    kind: ExportKind,
    split: ?[]const u8 = null,
    records: ?usize = null,
    size: u64,
    digest: []const u8,
};

pub const DatasetManifest = struct {
    schema_version: usize,
    family: []const u8,
    description: ?[]const u8 = null,
    files: []FileEntry,
    asset_dirs: ?[][]const u8 = null,
};

pub const LoadedManifest = struct {
    arena: std.heap.ArenaAllocator,
    value: DatasetManifest,

    pub fn deinit(self: *LoadedManifest) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub fn load(allocator: std.mem.Allocator, manifest_path: []const u8) !LoadedManifest {
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const arena_alloc = arena.allocator();

    const data = try compat.cwd().readFileAlloc(compat.io(), manifest_path, arena_alloc, .limited(16 * 1024 * 1024));
    const parsed = try std.json.parseFromSliceLeaky(DatasetManifest, arena_alloc, data, .{
        .ignore_unknown_fields = true,
    });

    return .{
        .arena = arena,
        .value = parsed,
    };
}

test "load manifest" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const manifest_json =
        \\{
        \\  "schema_version": 1,
        \\  "family": "gliner",
        \\  "description": "GLiNER export",
        \\  "files": [
        \\    {
        \\      "relative_path": "train-00000.jsonl",
        \\      "kind": "jsonl",
        \\      "split": "train",
        \\      "records": 10,
        \\      "size": 128,
        \\      "digest": "sha256:deadbeef"
        \\    }
        \\  ]
        \\}
    ;

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "manifest.json", .data = manifest_json });
    const path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "manifest.json" });
    defer allocator.free(path);

    var loaded = try load(allocator, path);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(usize, 1), loaded.value.schema_version);
    try std.testing.expectEqualStrings("gliner", loaded.value.family);
    try std.testing.expectEqual(@as(usize, 1), loaded.value.files.len);
    try std.testing.expectEqual(ExportKind.jsonl, loaded.value.files[0].kind);
    try std.testing.expectEqualStrings("train", loaded.value.files[0].split.?);
}
