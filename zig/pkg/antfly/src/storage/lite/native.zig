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

//! Native single-file Antfly Lite format primitives.
//!
//! This module owns the v1-native `.aflite` on-disk header and checkpoint-slot
//! layout. The higher-level Lite backend still routes DB traffic through the
//! bridge implementation until the native page/segment stores exist.

const std = @import("std");

const Allocator = std.mem.Allocator;

pub const magic = "AFLITE\x02N";
pub const format_version: u32 = 1;
pub const default_page_size: u32 = 4096;
pub const header_size: usize = 4096;
pub const checkpoint_slot_count = 2;
pub const checkpoint_slot_size: usize = 64;

const magic_offset: usize = 0;
const version_offset: usize = 8;
const page_size_offset: usize = 12;
const header_size_offset: usize = 16;
const active_checkpoint_offset: usize = 20;
const checkpoint_slots_offset: usize = 64;
const header_checksum_offset: usize = header_size - 4;

pub const CheckpointSlot = struct {
    commit_sequence: u64 = 0,
    catalog_root_page: u64 = 0,
    document_root_page: u64 = 0,
    index_catalog_root_page: u64 = 0,
    free_map_root_page: u64 = 0,
    page_count: u64 = 1,
};

pub const Header = struct {
    page_size: u32 = default_page_size,
    active_checkpoint: u8 = 0,
    checkpoints: [checkpoint_slot_count]CheckpointSlot = .{ .{}, .{} },
};

pub const InspectReport = struct {
    valid: bool,
    format_version: u32,
    page_size: u32,
    active_checkpoint: u8,
    commit_sequence: u64,
    page_count: u64,
    issue: ?[]const u8 = null,
};

pub fn create(io: std.Io, path: []const u8) !void {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});

    var file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &file_buf);
    try writer.interface.writeAll(&encoded);
    try writer.end();
    try file.sync(io);
}

pub fn inspect(_: Allocator, io: std.Io, path: []const u8) !InspectReport {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only });
    defer file.close(io);

    var header_bytes: [header_size]u8 = undefined;
    var reader = file.reader(io, &.{});
    try reader.interface.readSliceAll(&header_bytes);
    return inspectBytes(&header_bytes);
}

pub fn inspectBytes(raw: []const u8) InspectReport {
    const header = decodeHeader(raw) catch |err| {
        return .{
            .valid = false,
            .format_version = 0,
            .page_size = 0,
            .active_checkpoint = 0,
            .commit_sequence = 0,
            .page_count = 0,
            .issue = issueForDecodeError(err),
        };
    };
    const active = header.checkpoints[header.active_checkpoint];
    return .{
        .valid = true,
        .format_version = format_version,
        .page_size = header.page_size,
        .active_checkpoint = header.active_checkpoint,
        .commit_sequence = active.commit_sequence,
        .page_count = active.page_count,
    };
}

pub fn encodeHeader(out: *[header_size]u8, header: Header) void {
    @memset(out, 0);
    @memcpy(out[magic_offset..][0..magic.len], magic);
    std.mem.writeInt(u32, out[version_offset..][0..4], format_version, .little);
    std.mem.writeInt(u32, out[page_size_offset..][0..4], header.page_size, .little);
    std.mem.writeInt(u32, out[header_size_offset..][0..4], header_size, .little);
    out[active_checkpoint_offset] = header.active_checkpoint;

    for (header.checkpoints, 0..) |slot, index| {
        encodeCheckpointSlot(out[checkpointOffset(index)..][0..checkpoint_slot_size], slot);
    }

    std.mem.writeInt(u32, out[header_checksum_offset..][0..4], headerChecksum(out), .little);
}

pub fn decodeHeader(raw: []const u8) !Header {
    if (raw.len < header_size) return error.TruncatedNativeHeader;
    const header_raw = raw[0..header_size];
    if (!std.mem.eql(u8, header_raw[magic_offset..][0..magic.len], magic)) return error.InvalidNativeMagic;

    const version = std.mem.readInt(u32, header_raw[version_offset..][0..4], .little);
    if (version != format_version) return error.UnsupportedNativeFormatVersion;

    const encoded_header_size = std.mem.readInt(u32, header_raw[header_size_offset..][0..4], .little);
    if (encoded_header_size != header_size) return error.InvalidNativeHeaderSize;

    const expected_checksum = std.mem.readInt(u32, header_raw[header_checksum_offset..][0..4], .little);
    if (expected_checksum != headerChecksum(header_raw)) return error.NativeHeaderChecksumMismatch;

    const page_size = std.mem.readInt(u32, header_raw[page_size_offset..][0..4], .little);
    if (!validPageSize(page_size)) return error.InvalidNativePageSize;

    const active_checkpoint = header_raw[active_checkpoint_offset];
    if (active_checkpoint >= checkpoint_slot_count) return error.InvalidNativeCheckpointSlot;

    var checkpoints: [checkpoint_slot_count]CheckpointSlot = undefined;
    for (&checkpoints, 0..) |*slot, index| {
        slot.* = decodeCheckpointSlot(header_raw[checkpointOffset(index)..][0..checkpoint_slot_size]);
    }

    return .{
        .page_size = page_size,
        .active_checkpoint = active_checkpoint,
        .checkpoints = checkpoints,
    };
}

fn checkpointOffset(index: usize) usize {
    return checkpoint_slots_offset + index * checkpoint_slot_size;
}

fn encodeCheckpointSlot(out: []u8, slot: CheckpointSlot) void {
    std.debug.assert(out.len == checkpoint_slot_size);
    std.mem.writeInt(u64, out[0..8], slot.commit_sequence, .little);
    std.mem.writeInt(u64, out[8..16], slot.catalog_root_page, .little);
    std.mem.writeInt(u64, out[16..24], slot.document_root_page, .little);
    std.mem.writeInt(u64, out[24..32], slot.index_catalog_root_page, .little);
    std.mem.writeInt(u64, out[32..40], slot.free_map_root_page, .little);
    std.mem.writeInt(u64, out[40..48], slot.page_count, .little);
}

fn decodeCheckpointSlot(raw: []const u8) CheckpointSlot {
    std.debug.assert(raw.len == checkpoint_slot_size);
    return .{
        .commit_sequence = std.mem.readInt(u64, raw[0..8], .little),
        .catalog_root_page = std.mem.readInt(u64, raw[8..16], .little),
        .document_root_page = std.mem.readInt(u64, raw[16..24], .little),
        .index_catalog_root_page = std.mem.readInt(u64, raw[24..32], .little),
        .free_map_root_page = std.mem.readInt(u64, raw[32..40], .little),
        .page_count = std.mem.readInt(u64, raw[40..48], .little),
    };
}

fn headerChecksum(raw: []const u8) u32 {
    var crc = std.hash.Crc32.init();
    crc.update(raw[0..header_checksum_offset]);
    return crc.final();
}

fn validPageSize(page_size: u32) bool {
    return page_size >= 4096 and page_size <= 65536 and std.math.isPowerOfTwo(page_size);
}

fn issueForDecodeError(err: anyerror) []const u8 {
    return switch (err) {
        error.TruncatedNativeHeader => "truncated_header",
        error.InvalidNativeMagic => "invalid_magic",
        error.UnsupportedNativeFormatVersion => "unsupported_format_version",
        error.InvalidNativeHeaderSize => "invalid_header_size",
        error.NativeHeaderChecksumMismatch => "header_checksum_mismatch",
        error.InvalidNativePageSize => "invalid_page_size",
        error.InvalidNativeCheckpointSlot => "invalid_checkpoint_slot",
        else => "invalid_header",
    };
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite native header round trips initial checkpoint" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});

    const header = try decodeHeader(&encoded);
    try std.testing.expectEqual(default_page_size, header.page_size);
    try std.testing.expectEqual(@as(u8, 0), header.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 0), header.checkpoints[0].commit_sequence);
    try std.testing.expectEqual(@as(u64, 1), header.checkpoints[0].page_count);

    const report = inspectBytes(&encoded);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
    try std.testing.expectEqual(@as(u64, 1), report.page_count);
}

test "lite native header rejects corrupted checksum" {
    var encoded: [header_size]u8 = undefined;
    encodeHeader(&encoded, .{});
    encoded[page_size_offset] ^= 0xff;

    const report = inspectBytes(&encoded);
    try std.testing.expect(!report.valid);
    try std.testing.expectEqualStrings("header_checksum_mismatch", report.issue.?);
}

test "lite native create writes inspectable aflite file" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
    try std.testing.expectEqual(default_page_size, report.page_size);
    try std.testing.expectEqual(@as(u8, 0), report.active_checkpoint);
    try std.testing.expectEqual(@as(u64, 0), report.commit_sequence);
    try std.testing.expectEqual(@as(u64, 1), report.page_count);
}

test "lite native inspect reads only the header page" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-with-pages.aflite");
    defer allocator.free(path);

    try create(std.testing.io, path);
    {
        var file = try std.Io.Dir.cwd().createFile(std.testing.io, path, .{ .truncate = false });
        defer file.close(std.testing.io);
        const size = (try file.stat(std.testing.io)).size;
        var writer = file.writer(std.testing.io, &.{});
        try writer.seekTo(size);
        try writer.interface.writeAll("future-page-data");
        try writer.end();
    }

    const report = try inspect(allocator, std.testing.io, path);
    try std.testing.expect(report.valid);
    try std.testing.expectEqual(format_version, report.format_version);
}
