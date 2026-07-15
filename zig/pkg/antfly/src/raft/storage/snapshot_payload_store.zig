// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const fs_paths = @import("../../common/fs_paths.zig");

var publish_nonce = std.atomic.Value(u64).init(1);

pub fn writeAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    data: []const u8,
) !void {
    try fs_paths.createDirPathPortable(io, snapshot_dir);
    const final_path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(final_path);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{
        final_path,
        publish_nonce.fetchAdd(1, .monotonic),
    });
    defer alloc.free(tmp_path);
    errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(data);
        try writer.end();
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), final_path, io);
    try fs_paths.syncDirPortable(io, snapshot_dir);
}

pub fn writeArtifactAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    artifact: anytype,
) !void {
    try fs_paths.createDirPathPortable(io, snapshot_dir);
    const final_path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(final_path);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{
        final_path,
        publish_nonce.fetchAdd(1, .monotonic),
    });
    defer alloc.free(tmp_path);
    errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};

    {
        var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buffer: [64 * 1024]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try artifact.writeTo(&writer.interface);
        try writer.end();
        try file.sync(io);
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), final_path, io);
    try fs_paths.syncDirPortable(io, snapshot_dir);
}

pub fn readAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
) ![]u8 {
    const path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(path);
    return try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .unlimited);
}

pub fn delete(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
) void {
    if (index == 0) return;
    const path = pathAlloc(alloc, snapshot_dir, index, term) catch return;
    defer alloc.free(path);
    std.Io.Dir.cwd().deleteFile(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => std.log.warn("raft snapshot payload cleanup failed path={s} error={s}", .{ path, @errorName(err) }),
    };
}

pub fn cleanupOrphans(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    current_index: u64,
    current_term: u64,
) !void {
    const keep_name = if (current_index == 0)
        null
    else
        try std.fmt.allocPrint(alloc, "state-{d}-{d}.snap", .{ current_index, current_term });
    defer if (keep_name) |name| alloc.free(name);

    var dir = std.Io.Dir.cwd().openDir(io, snapshot_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer dir.close(io);

    var iter = dir.iterateAssumeFirstIteration();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file or !isManagedPayloadName(entry.name)) continue;
        if (keep_name) |name| {
            if (std.mem.eql(u8, entry.name, name)) continue;
        }
        dir.deleteFile(io, entry.name) catch |err| switch (err) {
            error.FileNotFound => {},
            else => std.log.warn("raft orphan snapshot payload cleanup failed path={s}/{s} error={s}", .{
                snapshot_dir,
                entry.name,
                @errorName(err),
            }),
        };
    }
}

fn isManagedPayloadName(name: []const u8) bool {
    if (!std.mem.startsWith(u8, name, "state-")) return false;
    const snapshot_suffix = std.mem.indexOf(u8, name, ".snap") orelse return false;
    const identity = name["state-".len..snapshot_suffix];
    const separator = std.mem.indexOfScalar(u8, identity, '-') orelse return false;
    if (separator == 0 or separator + 1 == identity.len) return false;
    _ = std.fmt.parseInt(u64, identity[0..separator], 10) catch return false;
    _ = std.fmt.parseInt(u64, identity[separator + 1 ..], 10) catch return false;

    const remainder = name[snapshot_suffix + ".snap".len ..];
    if (remainder.len == 0) return true;
    if (!std.mem.startsWith(u8, remainder, ".tmp-") or remainder.len == ".tmp-".len) return false;
    _ = std.fmt.parseInt(u64, remainder[".tmp-".len..], 10) catch return false;
    return true;
}

pub fn pathAlloc(alloc: std.mem.Allocator, snapshot_dir: []const u8, index: u64, term: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/state-{d}-{d}.snap", .{ snapshot_dir, index, term });
}

test "raft snapshot payload cleanup retains only the durable identity" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const snapshot_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_dir);

    try writeAtomically(std.testing.allocator, io, snapshot_dir, 7, 2, "old");
    try writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "current");
    const stale_tmp = try std.fmt.allocPrint(std.testing.allocator, "{s}/state-10-4.snap.tmp-1", .{snapshot_dir});
    defer std.testing.allocator.free(stale_tmp);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = stale_tmp, .data = "partial" });

    try cleanupOrphans(std.testing.allocator, io, snapshot_dir, 9, 3);
    const current = try readAlloc(std.testing.allocator, io, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(current);
    try std.testing.expectEqualStrings("current", current);

    const old_path = try pathAlloc(std.testing.allocator, snapshot_dir, 7, 2);
    defer std.testing.allocator.free(old_path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, old_path, .{}));
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, stale_tmp, .{}));
}
