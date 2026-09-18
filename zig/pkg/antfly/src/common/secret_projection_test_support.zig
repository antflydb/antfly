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
const builtin = @import("builtin");
const secrets = @import("secrets.zig");

pub const Projection = struct {
    tmp: std.testing.TmpDir,
    root: [:0]u8,
    path: []u8,
    io: std.Io,

    pub fn init(io: std.Io, key: []const u8) !Projection {
        if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
            return error.SkipZigTest;
        const alloc = std.testing.allocator;
        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        errdefer alloc.free(root);
        const path = try std.fs.path.join(alloc, &.{ root, "secrets.json" });
        errdefer alloc.free(path);

        inline for (.{ "..2026_01", "..2026_02" }, .{ "first", "other" }, .{ "a", "b" }) |dir, value, generation| {
            try tmp.dir.createDir(io, dir, .default_dir);
            const content = try std.fmt.allocPrint(alloc,
                \\{{"generation":"{s}","secrets":[{{"key":"{s}","value":"{s}","created_at_ns":1,"updated_at_ns":1}},{{"key":"projected.precedence","value":"{s}","created_at_ns":1,"updated_at_ns":1}}]}}
            , .{ generation ** 64, key, value, key });
            defer alloc.free(content);
            try tmp.dir.writeFile(io, .{ .sub_path = dir ++ "/secrets.json", .data = content });
        }
        // Equal size and mtime force reload detection to notice the new inode.
        const first = try tmp.dir.statFile(io, "..2026_01/secrets.json", .{});
        try tmp.dir.setTimestamps(io, "..2026_02/secrets.json", .{ .modify_timestamp = .{ .new = first.mtime } });
        const second = try tmp.dir.statFile(io, "..2026_02/secrets.json", .{});
        try std.testing.expectEqual(first.size, second.size);
        try std.testing.expectEqual(first.mtime, second.mtime);
        try std.testing.expect(first.inode != second.inode);
        try tmp.dir.symLink(io, "..2026_01", "..data", .{ .is_directory = true });
        try tmp.dir.symLink(io, "..data/secrets.json", "secrets.json", .{});
        return .{ .tmp = tmp, .root = root, .path = path, .io = io };
    }

    pub fn deinit(self: *Projection) void {
        std.testing.allocator.free(self.path);
        std.testing.allocator.free(self.root);
        self.tmp.cleanup();
    }

    pub fn rotate(self: *Projection) !void {
        try self.tmp.dir.symLink(self.io, "..2026_02", "..data-next", .{ .is_directory = true });
        try self.tmp.dir.rename("..data-next", self.tmp.dir, "..data", self.io);
        try self.tmp.dir.deleteTree(self.io, "..2026_01");
    }
};

pub fn expectValue(store: *secrets.FileStore, key: []const u8, expected: []const u8) !void {
    const value = (try store.getOwned(std.testing.allocator, key)).?;
    defer std.testing.allocator.free(value);
    try std.testing.expectEqualStrings(expected, value);
}

pub fn expectHealthyRotation(store: *secrets.FileStore, initial_generation: u64) !void {
    try std.testing.expectEqual(initial_generation + 1, store.generation());
    const health = store.healthSnapshot();
    try std.testing.expect(!health.last_reload_failed);
    try std.testing.expect(!health.stale_snapshot);
    try std.testing.expectEqual(@as(u64, 0), health.reload_failures);
    try std.testing.expect(health.supports_source_generation);
    try std.testing.expectEqualSlices(u8, &([_]u8{0xbb} ** 32), &health.source_generation.?);
}

// Call the runtime initializer so a regression cannot hide behind FileStore's
// existing direct-construction symlink tests.
pub fn expectRuntimeRotation(comptime init_store: anytype) !void {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var primary = try Projection.init(io_impl.io(), "projected.primary");
    defer primary.deinit();
    var fallback = try Projection.init(io_impl.io(), "projected.fallback");
    defer fallback.deinit();
    var store = try init_store(alloc, io_impl.io(), &.{ primary.path, fallback.path });
    defer store.deinit();
    try expectValue(&store, "projected.primary", "first");
    try expectValue(&store, "projected.fallback", "first");
    try expectValue(&store, "projected.precedence", "projected.primary");
    const initial_generation = store.generation();
    const fallback_generation = store.fallbacks[0].generation();

    try fallback.rotate();
    try expectValue(&store, "projected.fallback", "other");
    try expectValue(&store, "projected.precedence", "projected.primary");
    try expectHealthyRotation(&store.fallbacks[0], fallback_generation);
    try primary.rotate();
    try expectValue(&store, "projected.primary", "other");
    try expectValue(&store, "projected.precedence", "projected.primary");
    try std.testing.expectEqualStrings(primary.path, store.path);
    try std.testing.expectEqualStrings(fallback.path, store.fallbacks[0].path);
    try std.testing.expectEqual(initial_generation + 3, store.generation());
    const health = store.healthSnapshot();
    try std.testing.expect(!health.last_reload_failed);
    try std.testing.expect(!health.stale_snapshot);
    try std.testing.expectEqual(@as(u64, 0), health.reload_failures);
}

pub fn expectRuntimeWrites(comptime init_store: anytype) !void {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const cwd = try std.Io.Dir.cwd().realPathFileAlloc(io, ".", alloc);
    defer alloc.free(cwd);

    for ([_]bool{ false, true }) |relative| {
        var primary = try Projection.init(io, "projected.primary");
        defer primary.deinit();
        var fallback = try Projection.init(io, "projected.fallback");
        defer fallback.deinit();
        const path = if (relative)
            try std.fs.path.relative(alloc, cwd, null, cwd, primary.path)
        else
            try alloc.dupe(u8, primary.path);
        defer alloc.free(path);
        var store = try init_store(alloc, io, &.{ path, fallback.path });
        defer store.deinit();

        // Each generation must remain reachable through the logical symlink
        // after both mutation operations. Reopening the target verifies disk
        // contents independently of the writer's in-memory snapshot.
        inline for (.{ "..2026_01", "..2026_02" }, 0..) |dir, index| {
            if (index != 0) {
                const generation = store.generation();
                try primary.rotate();
                try expectValue(&store, "projected.primary", "other");
                try std.testing.expectEqual(generation + 1, store.generation());
            }
            const target_path = try std.fs.path.join(alloc, &.{ primary.root, dir, "secrets.json" });
            defer alloc.free(target_path);
            var target = try secrets.FileStore.initWithIo(alloc, io, target_path);
            defer target.deinit();

            var listed = try store.put(alloc, "projected.primary", "updated");
            defer listed.deinit(alloc);
            try expectValue(&target, "projected.primary", "updated");
            try expectValue(&store, "projected.primary", "updated");
            try std.testing.expectEqual(.sym_link, (try primary.tmp.dir.statFile(io, "secrets.json", .{ .follow_symlinks = false })).kind);

            try std.testing.expect(try store.delete("projected.precedence"));
            const deleted = try target.getOwned(alloc, "projected.precedence");
            defer if (deleted) |value| alloc.free(value);
            try std.testing.expectEqual(@as(?[]u8, null), deleted);
            try expectValue(&store, "projected.precedence", "projected.fallback");
            try expectValue(&store, "projected.fallback", "first");
            try std.testing.expectEqual(.sym_link, (try primary.tmp.dir.statFile(io, "secrets.json", .{ .follow_symlinks = false })).kind);
        }
        try std.testing.expectEqualStrings(path, store.path);
        try std.testing.expect(!store.healthSnapshot().last_reload_failed);
    }
}
