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
    const current = loadPath(alloc, io, path) catch |err| switch (err) {
        error.FileNotFound => {
            const created = try newState(io);
            try writePath(alloc, io, path, created);
            return created;
        },
        else => return err,
    };
    return current;
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
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{d}", .{ path, platform_time.monotonicNs() });
    defer alloc.free(tmp_path);
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
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
