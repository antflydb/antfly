// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Durable identity for one physical DB root.
//!
//! This state belongs to the primary storage lifecycle, not any derived
//! projection. A staged physical root creates this checkpoint before it is
//! atomically published; ordinary opens only load the path-owned identity.

const std = @import("std");
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const platform_time = @import("antfly_platform").time;

const file_name = "root_identity.checkpoint";
const creation_lock_name = "root_identity.checkpoint.lock";
const magic = "AFROOTI1";
const format_version: u32 = 1;
const encoded_len = magic.len + @sizeOf(u32) + @sizeOf(u128) + @sizeOf(u32);

pub const State = struct {
    incarnation: u128,
};

pub fn checkpointPathAlloc(alloc: Allocator, db_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db_path, file_name });
}

pub fn loadOrCreate(alloc: Allocator, io: std.Io, db_path: []const u8) !State {
    const path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(path);
    return loadPath(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => try createLocked(alloc, io, db_path, path),
        else => return err,
    };
}

pub fn load(alloc: Allocator, io: std.Io, db_path: []const u8) !State {
    const path = try checkpointPathAlloc(alloc, db_path);
    defer alloc.free(path);
    return try loadPath(alloc, io, path);
}

fn newState(io: std.Io) !State {
    var entropy: [16]u8 = undefined;
    try io.randomSecure(&entropy);
    var incarnation = std.mem.readInt(u128, &entropy, .little);
    if (incarnation == 0) incarnation = 1;
    return .{ .incarnation = incarnation };
}

fn createLocked(alloc: Allocator, io: std.Io, db_path: []const u8, path: []const u8) !State {
    fs_paths.createDirPathPortable(io, db_path) catch |err| switch (err) {
        error.FileNotFound => return error.RootIdentityDirectoryUnavailable,
        else => return err,
    };
    const lock_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ db_path, creation_lock_name });
    defer alloc.free(lock_path);
    // Darwin's atomic open-and-lock flags can report ENOENT when two O_CREAT
    // callers race on a missing lock file. Materialize the stable lock inode
    // first, then acquire the advisory lock in a separate open.
    if (fs_paths.createFilePortable(io, lock_path, .{ .truncate = false, .exclusive = true })) |lock_seed| {
        lock_seed.close(io);
    } else |err| switch (err) {
        error.PathAlreadyExists => {},
        error.FileNotFound => return error.RootIdentityLockSeedUnavailable,
        else => return err,
    }
    const creation_lock = fs_paths.createFilePortable(io, lock_path, .{
        .read = true,
        .truncate = false,
        .lock = .exclusive,
        .lock_nonblocking = false,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.RootIdentityLockUnavailable,
        else => return err,
    };
    defer creation_lock.close(io);

    // Another process may have completed creation while this opener waited.
    return loadPath(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => {
            const created = try newState(io);
            writePath(alloc, io, path, created) catch |write_err| switch (write_err) {
                error.FileNotFound => return error.RootIdentityWritePathUnavailable,
                else => return write_err,
            };
            return created;
        },
        else => return err,
    };
}

fn loadPath(alloc: Allocator, io: std.Io, path: []const u8) !State {
    // `limited` reserves one byte to distinguish an exact-size payload from a
    // truncated oversized file. Read at most one extra byte and let `decode`
    // enforce the exact checkpoint size.
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(encoded_len + 1));
    defer alloc.free(raw);
    return try decode(raw);
}

fn writePath(alloc: Allocator, io: std.Io, path: []const u8, state: State) !void {
    const encoded = encode(state);
    if (std.fs.path.dirname(path)) |parent| {
        try fs_paths.createDirPathPortable(io, parent);
    }
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{x}", .{ path, state.incarnation });
    defer alloc.free(tmp_path);
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true, .exclusive = true });
        defer file.close(io);
        var buffer: [encoded_len]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(&encoded);
        try writer.end();
        try file.sync(io);
    }
    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

fn encode(state: State) [encoded_len]u8 {
    var raw: [encoded_len]u8 = undefined;
    var pos: usize = 0;
    @memcpy(raw[pos..][0..magic.len], magic);
    pos += magic.len;
    std.mem.writeInt(u32, raw[pos..][0..@sizeOf(u32)], format_version, .little);
    pos += @sizeOf(u32);
    std.mem.writeInt(u128, raw[pos..][0..@sizeOf(u128)], state.incarnation, .little);
    pos += @sizeOf(u128);
    std.mem.writeInt(u32, raw[pos..][0..@sizeOf(u32)], std.hash.Crc32.hash(raw[0..pos]), .little);
    return raw;
}

fn decode(raw: []const u8) !State {
    if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidRootIdentityState;
    const payload_end = raw.len - @sizeOf(u32);
    const expected_crc = std.mem.readInt(u32, raw[payload_end..][0..@sizeOf(u32)], .little);
    if (std.hash.Crc32.hash(raw[0..payload_end]) != expected_crc) return error.InvalidRootIdentityState;
    var pos: usize = magic.len;
    const version = std.mem.readInt(u32, raw[pos..][0..@sizeOf(u32)], .little);
    pos += @sizeOf(u32);
    if (version != format_version) return error.InvalidRootIdentityState;
    const incarnation = std.mem.readInt(u128, raw[pos..][0..@sizeOf(u128)], .little);
    if (incarnation == 0) return error.InvalidRootIdentityState;
    return .{ .incarnation = incarnation };
}

test "root identity rejects corruption" {
    const valid = encode(.{ .incarnation = 17 });
    var corrupt = valid;
    corrupt[magic.len + @sizeOf(u32)] ^= 0xff;
    try std.testing.expectError(error.InvalidRootIdentityState, decode(&corrupt));
}

test "root identity concurrent first opens publish one incarnation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/identity", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    try fs_paths.createDirPathPortable(std.testing.io, path);

    const Worker = struct {
        io: std.Io,
        path: []const u8,
        ready: *std.atomic.Value(u32),
        start: *std.atomic.Value(bool),
        state: ?State = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = self.ready.fetchAdd(1, .release);
            while (!self.start.load(.acquire)) platform_time.yieldBriefly();
            self.state = loadOrCreate(std.heap.page_allocator, self.io, self.path) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    var ready = std.atomic.Value(u32).init(0);
    var start = std.atomic.Value(bool).init(false);
    var first = Worker{ .io = std.testing.io, .path = path, .ready = &ready, .start = &start };
    var second = Worker{ .io = std.testing.io, .path = path, .ready = &ready, .start = &start };
    const first_thread = try std.Thread.spawn(.{}, Worker.run, .{&first});
    const second_thread = try std.Thread.spawn(.{}, Worker.run, .{&second});
    while (ready.load(.acquire) != 2) platform_time.yieldBriefly();
    start.store(true, .release);
    first_thread.join();
    second_thread.join();

    if (first.err) |err| return err;
    if (second.err) |err| return err;
    try std.testing.expectEqual(first.state.?.incarnation, second.state.?.incarnation);
    try std.testing.expectEqual(first.state.?.incarnation, (try load(std.testing.allocator, std.testing.io, path)).incarnation);
}
