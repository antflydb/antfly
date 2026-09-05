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
const legacy_version: u32 = 1;
const version: u32 = 2;
const max_index_name_bytes: usize = 4 * 1024;
const max_manifest_bytes: usize = 8 * 1024;

pub const PhysicalFormat = enum(u16) {
    /// Generic LSM-backed physical generation used by pre-native indexes and
    /// by non-dense derived indexes.
    legacy_lsm = 1,
    /// Native HBC WAL + immutable segment generation. V2 active-root pointers
    /// deliberately keep binaries that do not understand this format out.
    dense_native_v2 = 2,
};

pub const Manifest = struct {
    generation_id: u128,
    config_hash: u64,
    ready_applied_sequence: u64,
    physical_format: PhysicalFormat,
    index_name: []u8,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.index_name);
        self.* = undefined;
    }
};

/// Domain result for activation certification. Structural outcomes are kept
/// distinct from operational I/O errors so recovery code can quarantine a bad
/// generation without treating allocation, cancellation, or device failures
/// as evidence that the durable artifact itself is invalid.
pub const ReadyCertification = union(enum) {
    ready: u64,
    missing,
    invalid,
    identity_mismatch,
};

pub fn writeReady(
    alloc: Allocator,
    index_path: []const u8,
    generation_id: u128,
    index_name: []const u8,
    config_hash: u64,
    ready_applied_sequence: u64,
) !void {
    return writeReadyForPhysicalFormat(
        alloc,
        index_path,
        generation_id,
        index_name,
        config_hash,
        ready_applied_sequence,
        .legacy_lsm,
    );
}

pub fn writeReadyForPhysicalFormat(
    alloc: Allocator,
    index_path: []const u8,
    generation_id: u128,
    index_name: []const u8,
    config_hash: u64,
    ready_applied_sequence: u64,
    physical_format: PhysicalFormat,
) !void {
    if (generation_id == 0 or index_name.len == 0 or index_name.len > max_index_name_bytes) {
        return error.InvalidIndexGenerationManifest;
    }
    const path = try manifestPathAlloc(alloc, index_path);
    defer alloc.free(path);
    const raw = try encode(alloc, generation_id, index_name, config_hash, ready_applied_sequence, physical_format);
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
    return switch (try certifyReady(
        alloc,
        index_path,
        expected_generation_id,
        expected_index_name,
        expected_config_hash,
    )) {
        .ready => |sequence| sequence,
        .missing => error.FileNotFound,
        .invalid => error.InvalidIndexGenerationManifest,
        .identity_mismatch => error.IndexGenerationManifestMismatch,
    };
}

pub fn certifyReady(
    alloc: Allocator,
    index_path: []const u8,
    expected_generation_id: ?u128,
    expected_index_name: []const u8,
    expected_config_hash: u64,
) !ReadyCertification {
    var manifest = load(alloc, index_path) catch |err| switch (err) {
        error.FileNotFound => return .missing,
        error.InvalidIndexGenerationManifest,
        error.FileTooBig,
        error.StreamTooLong,
        => return .invalid,
        else => return err,
    };
    defer manifest.deinit(alloc);
    if (expected_generation_id) |generation_id| {
        if (manifest.generation_id != generation_id) return .identity_mismatch;
    }
    if (!std.mem.eql(u8, manifest.index_name, expected_index_name) or
        manifest.config_hash != expected_config_hash)
    {
        return .identity_mismatch;
    }
    return .{ .ready = manifest.ready_applied_sequence };
}

fn manifestPathAlloc(alloc: Allocator, index_path: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}/{s}", .{ index_path, file_name });
}

fn encode(
    alloc: Allocator,
    generation_id: u128,
    index_name: []const u8,
    config_hash: u64,
    sequence: u64,
    physical_format: PhysicalFormat,
) ![]u8 {
    return encodeForVersion(
        alloc,
        generation_id,
        index_name,
        config_hash,
        sequence,
        physical_format,
        version,
    );
}

fn encodeForVersion(
    alloc: Allocator,
    generation_id: u128,
    index_name: []const u8,
    config_hash: u64,
    sequence: u64,
    physical_format: PhysicalFormat,
    record_version: u32,
) ![]u8 {
    if (record_version != legacy_version and record_version != version) {
        return error.InvalidIndexGenerationManifest;
    }
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, magic);
    try appendInt(alloc, &out, u32, record_version);
    try appendInt(alloc, &out, u128, generation_id);
    try appendInt(alloc, &out, u64, config_hash);
    try appendInt(alloc, &out, u64, sequence);
    if (record_version >= version) {
        try appendInt(alloc, &out, u16, @intFromEnum(physical_format));
    }
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
    const record_version = try readInt(raw[0..payload_end], &pos, u32);
    if (record_version != legacy_version and record_version != version) return error.InvalidIndexGenerationManifest;
    const generation_id = try readInt(raw[0..payload_end], &pos, u128);
    const config_hash = try readInt(raw[0..payload_end], &pos, u64);
    const sequence = try readInt(raw[0..payload_end], &pos, u64);
    const physical_format: PhysicalFormat = if (record_version >= version) switch (try readInt(
        raw[0..payload_end],
        &pos,
        u16,
    )) {
        @intFromEnum(PhysicalFormat.legacy_lsm) => .legacy_lsm,
        @intFromEnum(PhysicalFormat.dense_native_v2) => .dense_native_v2,
        else => return error.InvalidIndexGenerationManifest,
    } else .legacy_lsm;
    const name_len = try readInt(raw[0..payload_end], &pos, u32);
    if (generation_id == 0 or name_len == 0 or name_len > max_index_name_bytes or pos + name_len != payload_end) {
        return error.InvalidIndexGenerationManifest;
    }
    return .{
        .generation_id = generation_id,
        .config_hash = config_hash,
        .ready_applied_sequence = sequence,
        .physical_format = physical_format,
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

    const mismatched = try certifyReady(alloc, path, 92, "dense_idx", 44);
    try std.testing.expectEqual(ReadyCertification.identity_mismatch, std.meta.activeTag(mismatched));
    const manifest_path = try manifestPathAlloc(alloc, path);
    defer alloc.free(manifest_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = manifest_path,
        .data = "invalid",
    });
    const invalid = try certifyReady(alloc, path, 91, "dense_idx", 44);
    try std.testing.expectEqual(ReadyCertification.invalid, std.meta.activeTag(invalid));
    try std.Io.Dir.cwd().deleteFile(std.testing.io, manifest_path);
    const missing = try certifyReady(alloc, path, 91, "dense_idx", 44);
    try std.testing.expectEqual(ReadyCertification.missing, std.meta.activeTag(missing));
}

test "index generation manifest identifies native physical generations" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    try writeReadyForPhysicalFormat(alloc, path, 92, "dense_idx", 45, 124, .dense_native_v2);
    var manifest = try load(alloc, path);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(PhysicalFormat.dense_native_v2, manifest.physical_format);
}

test "index generation manifest decodes legacy v1 as LSM physical format" {
    const alloc = std.testing.allocator;
    const raw = try encodeForVersion(alloc, 93, "old_dense_idx", 46, 125, .legacy_lsm, legacy_version);
    defer alloc.free(raw);
    var manifest = try decode(alloc, raw);
    defer manifest.deinit(alloc);
    try std.testing.expectEqual(PhysicalFormat.legacy_lsm, manifest.physical_format);
    try std.testing.expectEqual(@as(u64, 125), manifest.ready_applied_sequence);
}
