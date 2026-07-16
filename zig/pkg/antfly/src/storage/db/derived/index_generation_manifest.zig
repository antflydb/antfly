// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Durable readiness identity for a shadow index generation.
//!
//! The active-root pointer may select a shadow only after this checksummed,
//! fsync+rename manifest exists. A directory alone is never publication proof.

const std = @import("std");
const fs_paths = @import("../../../common/fs_paths.zig");

const Allocator = std.mem.Allocator;
const file_name = ".antfly-index-generation";
const magic = "AFIDXGN1";
const version: u32 = 1;
const max_index_name_bytes: usize = 4 * 1024;
const max_manifest_bytes: usize = 8 * 1024;

pub const Manifest = struct {
    generation_id: u128,
    config_hash: u64,
    ready_applied_sequence: u64,
    index_name: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        self.* = undefined;
    }
};

pub fn writeReady(
    alloc: Allocator,
    index_path: []const u8,
    generation_id: u128,
    index_name: []const u8,
    config_hash: u64,
    ready_applied_sequence: u64,
) !void {
    if (generation_id == 0 or index_name.len == 0 or index_name.len > max_index_name_bytes) {
        return error.InvalidIndexGenerationManifest;
    }
    const path = try manifestPathAlloc(alloc, index_path);
    defer alloc.free(path);
    const raw = try encode(alloc, generation_id, index_name, config_hash, ready_applied_sequence);
    defer alloc.free(raw);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp", .{path});
    defer alloc.free(tmp_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var writer_buffer: [4096]u8 = undefined;
        var writer = file.writer(io, &writer_buffer);
        try writer.interface.writeAll(raw);
        try writer.end();
        try file.sync(io);
    }
    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    try fs_paths.syncDirPortable(io, index_path);
}

pub fn load(alloc: Allocator, index_path: []const u8) !Manifest {
    const path = try manifestPathAlloc(alloc, index_path);
    defer alloc.free(path);
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const raw = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_manifest_bytes));
    defer alloc.free(raw);
    return try decode(alloc, raw);
}

pub fn validateReady(
    alloc: Allocator,
    index_path: []const u8,
    expected_generation_id: ?u128,
    expected_index_name: []const u8,
    expected_config_hash: u64,
) !u64 {
    var manifest = try load(alloc, index_path);
    defer manifest.deinit(alloc);
    if (expected_generation_id) |generation_id| {
        if (manifest.generation_id != generation_id) return error.IndexGenerationManifestMismatch;
    }
    if (!std.mem.eql(u8, manifest.index_name, expected_index_name) or
        manifest.config_hash != expected_config_hash)
    {
        return error.IndexGenerationManifestMismatch;
    }
    return manifest.ready_applied_sequence;
}

fn manifestPathAlloc(alloc: Allocator, index_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ index_path, file_name });
}

fn encode(alloc: Allocator, generation_id: u128, index_name: []const u8, config_hash: u64, sequence: u64) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, magic);
    try appendInt(alloc, &out, u32, version);
    try appendInt(alloc, &out, u128, generation_id);
    try appendInt(alloc, &out, u64, config_hash);
    try appendInt(alloc, &out, u64, sequence);
    try appendInt(alloc, &out, u32, @intCast(index_name.len));
    try out.appendSlice(alloc, index_name);
    try appendInt(alloc, &out, u32, std.hash.Crc32.hash(out.items));
    return try out.toOwnedSlice(alloc);
}

fn decode(alloc: Allocator, raw: []const u8) !Manifest {
    if (raw.len < magic.len + 4 + 16 + 8 + 8 + 4 + 4 or !std.mem.eql(u8, raw[0..magic.len], magic)) {
        return error.InvalidIndexGenerationManifest;
    }
    const payload_end = raw.len - 4;
    if (std.hash.Crc32.hash(raw[0..payload_end]) != std.mem.readInt(u32, raw[payload_end..][0..4], .little)) {
        return error.InvalidIndexGenerationManifest;
    }
    var pos: usize = magic.len;
    if (try readInt(raw[0..payload_end], &pos, u32) != version) return error.InvalidIndexGenerationManifest;
    const generation_id = try readInt(raw[0..payload_end], &pos, u128);
    const config_hash = try readInt(raw[0..payload_end], &pos, u64);
    const sequence = try readInt(raw[0..payload_end], &pos, u64);
    const name_len = try readInt(raw[0..payload_end], &pos, u32);
    if (generation_id == 0 or name_len == 0 or name_len > max_index_name_bytes or pos + name_len != payload_end) {
        return error.InvalidIndexGenerationManifest;
    }
    return .{
        .generation_id = generation_id,
        .config_hash = config_hash,
        .ready_applied_sequence = sequence,
        .index_name = try alloc.dupe(u8, raw[pos .. pos + name_len]),
    };
}

fn appendInt(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn readInt(raw: []const u8, pos: *usize, comptime T: type) !T {
    if (pos.* + @sizeOf(T) > raw.len) return error.InvalidIndexGenerationManifest;
    const value = std.mem.readInt(T, raw[pos.*..][0..@sizeOf(T)], .little);
    pos.* += @sizeOf(T);
    return value;
}

test "index generation manifest is durable and fenced by identity" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    try writeReady(alloc, path, 91, "dense_idx", 44, 123);
    try std.testing.expectEqual(@as(u64, 123), try validateReady(alloc, path, 91, "dense_idx", 44));
    try std.testing.expectError(error.IndexGenerationManifestMismatch, validateReady(alloc, path, 92, "dense_idx", 44));
    try std.testing.expectError(error.IndexGenerationManifestMismatch, validateReady(alloc, path, 91, "other", 44));
}
