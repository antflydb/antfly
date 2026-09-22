// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Admit auxiliary serving files once, then publish those exact bytes durably.
const std = @import("std");
const snapshot = @import("../runtime/file_snapshot.zig");
const names = [_][]const u8{ "tokenizer.json", "tokenizer_config.json", "special_tokens_map.json" };
pub const Assets = struct {
    bytes: [3]?[]const u8,

    /// Storage belongs to the caller's run-lifetime arena.
    pub fn read(a: std.mem.Allocator, io: std.Io, dir: std.Io.Dir) !Assets {
        var result = Assets{ .bytes = @splat(null) };
        for (names, 0..) |name, i| {
            const bytes = snapshot.read(a, io, dir, name, if (i == 0) 64 * 1024 * 1024 else 1024 * 1024, null) catch |err| {
                if (i != 0 and err == error.FileNotFound) continue;
                return err;
            };
            const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidLayaTokenizerMetadata;
            result.bytes[i] = bytes;
        }
        return result;
    }

    pub fn write(self: Assets, io: std.Io, dir: std.Io.Dir) !void {
        for (names, self.bytes) |name, maybe_bytes| if (maybe_bytes) |bytes| {
            const file = try dir.createFile(io, name, .{ .exclusive = true });
            defer file.close(io);
            try file.writeStreamingAll(io, bytes);
            try file.sync(io);
        };
    }
};

test "laya export preserves admitted tokenizer bytes and permits absent optional metadata" {
    const io = std.testing.io;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input = try temp.dir.createFile(io, "tokenizer.json", .{});
    try input.writeStreamingAll(io, "{\"original\":true}");
    input.close(io);
    const assets = try Assets.read(a, io, temp.dir);
    const changed = try temp.dir.createFile(io, "tokenizer.json", .{});
    try changed.writeStreamingAll(io, "{\"changed\":true}");
    changed.close(io);
    try temp.dir.createDir(io, "export", .default_dir);
    var dest = try temp.dir.openDir(io, "export", .{});
    defer dest.close(io);
    try assets.write(io, dest);
    const actual = try snapshot.read(a, io, dest, "tokenizer.json", 1024, null);
    try std.testing.expectEqualStrings("{\"original\":true}", actual);
    try std.testing.expectError(error.FileNotFound, dest.openFile(io, "tokenizer_config.json", .{}));
    const invalid = try temp.dir.createFile(io, "tokenizer_config.json", .{});
    try invalid.writeStreamingAll(io, "[]");
    invalid.close(io);
    try std.testing.expectError(error.InvalidLayaTokenizerMetadata, Assets.read(a, io, temp.dir));
}
