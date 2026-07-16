// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at https://www.antfly.io/licensing/ELv2-license.

const std = @import("std");
const fs_paths = @import("../../common/fs_paths.zig");

const Sha256 = std.crypto.hash.sha2.Sha256;
const envelope_magic = "AFRSPAY\x00";
const envelope_version: u32 = 1;
const envelope_header_len: usize = 72;
const digest_domain = "antfly-raft-snapshot-payload-v1\x00";
const copy_buffer_len = 64 * 1024;

var publish_nonce = std.atomic.Value(u64).init(1);

const SliceSource = struct {
    bytes: []const u8,

    fn len(self: @This()) u64 {
        return self.bytes.len;
    }

    fn writeTo(self: @This(), writer: *std.Io.Writer) !void {
        try writer.writeAll(self.bytes);
    }
};

const Envelope = struct {
    index: u64,
    term: u64,
    payload_len: u64,
    digest: [Sha256.digest_length]u8,
};

pub fn writeAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    data: []const u8,
) !void {
    try writeSourceAtomically(alloc, io, snapshot_dir, index, term, SliceSource{ .bytes = data });
}

pub fn writeArtifactAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    artifact: anytype,
) !void {
    try writeSourceAtomically(alloc, io, snapshot_dir, index, term, artifact);
}

fn writeSourceAtomically(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
    source: anytype,
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

    const payload_len = source.len();
    var published_envelope: Envelope = undefined;
    {
        var file = try std.Io.Dir.cwd().createFile(io, tmp_path, .{ .truncate = true });
        defer file.close(io);

        var file_buffer: [copy_buffer_len]u8 = undefined;
        var file_writer = file.writer(io, &file_buffer);
        try file_writer.interface.splatByteAll(0, envelope_header_len);

        var hash_buffer: [copy_buffer_len]u8 = undefined;
        var hashed = file_writer.interface.hashed(Sha256.init(.{}), &hash_buffer);
        updateDigestIdentity(&hashed.hasher, index, term, payload_len);
        try source.writeTo(&hashed.writer);
        try hashed.writer.flush();
        try file_writer.end();

        const expected_file_len = std.math.add(u64, envelope_header_len, payload_len) catch
            return error.SnapshotPayloadTooLarge;
        if (try file.length(io) != expected_file_len) return error.SnapshotArtifactSizeMismatch;

        var digest: [Sha256.digest_length]u8 = undefined;
        hashed.hasher.final(&digest);
        published_envelope = .{
            .index = index,
            .term = term,
            .payload_len = payload_len,
            .digest = digest,
        };
        const header = encodeEnvelope(published_envelope);
        try file.writePositionalAll(io, &header, 0);
        try file.sync(io);
    }

    if (try existingPayloadMatches(io, final_path, published_envelope)) {
        try std.Io.Dir.cwd().deleteFile(io, tmp_path);
        return;
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), final_path, io);
    try fs_paths.syncDirPortable(io, snapshot_dir);
}

fn existingPayloadMatches(io: std.Io, path: []const u8, expected: Envelope) !bool {
    var file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer file.close(io);
    const existing = try readEnvelope(file, io, expected.index, expected.term);
    try validateOpenFile(file, io, existing);
    if (existing.payload_len != expected.payload_len or
        !std.crypto.timing_safe.eql([Sha256.digest_length]u8, existing.digest, expected.digest))
    {
        return error.SnapshotPayloadIdentityConflict;
    }
    return true;
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
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const envelope = try readEnvelope(file, io, index, term);
    if (envelope.payload_len > std.math.maxInt(usize)) return error.SnapshotPayloadTooLarge;

    const payload = try alloc.alloc(u8, @intCast(envelope.payload_len));
    errdefer alloc.free(payload);
    if (try file.readPositionalAll(io, payload, envelope_header_len) != payload.len)
        return error.SnapshotPayloadSizeMismatch;
    try verifyDigest(envelope, payload);
    return payload;
}

pub fn validate(
    alloc: std.mem.Allocator,
    io: std.Io,
    snapshot_dir: []const u8,
    index: u64,
    term: u64,
) !void {
    const path = try pathAlloc(alloc, snapshot_dir, index, term);
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    const envelope = try readEnvelope(file, io, index, term);

    try validateOpenFile(file, io, envelope);
}

fn validateOpenFile(file: std.Io.File, io: std.Io, envelope: Envelope) !void {
    var hasher = Sha256.init(.{});
    updateDigestIdentity(&hasher, envelope.index, envelope.term, envelope.payload_len);
    var buffer: [copy_buffer_len]u8 = undefined;
    var offset: u64 = 0;
    while (offset < envelope.payload_len) {
        const wanted: usize = @intCast(@min(buffer.len, envelope.payload_len - offset));
        const read = try file.readPositionalAll(io, buffer[0..wanted], envelope_header_len + offset);
        if (read != wanted) return error.SnapshotPayloadSizeMismatch;
        hasher.update(buffer[0..read]);
        offset += read;
    }
    var actual: [Sha256.digest_length]u8 = undefined;
    hasher.final(&actual);
    if (!std.crypto.timing_safe.eql([Sha256.digest_length]u8, actual, envelope.digest))
        return error.SnapshotPayloadChecksumMismatch;
}

fn readEnvelope(file: std.Io.File, io: std.Io, expected_index: u64, expected_term: u64) !Envelope {
    if (try file.length(io) < envelope_header_len) return error.InvalidSnapshotPayloadHeader;
    var header: [envelope_header_len]u8 = undefined;
    if (try file.readPositionalAll(io, &header, 0) != header.len) return error.InvalidSnapshotPayloadHeader;
    if (!std.mem.eql(u8, header[0..envelope_magic.len], envelope_magic))
        return error.InvalidSnapshotPayloadHeader;
    if (std.mem.readInt(u32, header[8..12], .little) != envelope_version)
        return error.UnsupportedSnapshotPayloadVersion;
    if (std.mem.readInt(u32, header[12..16], .little) != envelope_header_len)
        return error.InvalidSnapshotPayloadHeader;

    const index = std.mem.readInt(u64, header[16..24], .little);
    const term = std.mem.readInt(u64, header[24..32], .little);
    const payload_len = std.mem.readInt(u64, header[32..40], .little);
    if (index != expected_index or term != expected_term) return error.SnapshotPayloadIdentityMismatch;
    const expected_file_len = std.math.add(u64, envelope_header_len, payload_len) catch
        return error.SnapshotPayloadTooLarge;
    if (try file.length(io) != expected_file_len) return error.SnapshotPayloadSizeMismatch;

    var digest: [Sha256.digest_length]u8 = undefined;
    @memcpy(&digest, header[40..72]);
    return .{ .index = index, .term = term, .payload_len = payload_len, .digest = digest };
}

fn encodeEnvelope(envelope: Envelope) [envelope_header_len]u8 {
    var header: [envelope_header_len]u8 = undefined;
    @memcpy(header[0..8], envelope_magic);
    std.mem.writeInt(u32, header[8..12], envelope_version, .little);
    std.mem.writeInt(u32, header[12..16], envelope_header_len, .little);
    std.mem.writeInt(u64, header[16..24], envelope.index, .little);
    std.mem.writeInt(u64, header[24..32], envelope.term, .little);
    std.mem.writeInt(u64, header[32..40], envelope.payload_len, .little);
    @memcpy(header[40..72], &envelope.digest);
    return header;
}

fn updateDigestIdentity(hasher: *Sha256, index: u64, term: u64, payload_len: u64) void {
    hasher.update(digest_domain);
    var encoded: [24]u8 = undefined;
    std.mem.writeInt(u64, encoded[0..8], index, .little);
    std.mem.writeInt(u64, encoded[8..16], term, .little);
    std.mem.writeInt(u64, encoded[16..24], payload_len, .little);
    hasher.update(&encoded);
}

fn verifyDigest(envelope: Envelope, payload: []const u8) !void {
    var hasher = Sha256.init(.{});
    updateDigestIdentity(&hasher, envelope.index, envelope.term, envelope.payload_len);
    hasher.update(payload);
    var actual: [Sha256.digest_length]u8 = undefined;
    hasher.final(&actual);
    if (!std.crypto.timing_safe.eql([Sha256.digest_length]u8, actual, envelope.digest))
        return error.SnapshotPayloadChecksumMismatch;
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
        if (keep_name) |name| if (std.mem.eql(u8, entry.name, name)) continue;
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

test "raft snapshot payload envelope validates identity length and checksum" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const snapshot_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_dir);

    try writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "current");
    try validate(std.testing.allocator, io, snapshot_dir, 9, 3);
    const current = try readAlloc(std.testing.allocator, io, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(current);
    try std.testing.expectEqualStrings("current", current);
    try writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "current");
    try std.testing.expectError(
        error.SnapshotPayloadIdentityConflict,
        writeAtomically(std.testing.allocator, io, snapshot_dir, 9, 3, "different"),
    );
    const unchanged = try readAlloc(std.testing.allocator, io, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(unchanged);
    try std.testing.expectEqualStrings("current", unchanged);
    const path = try pathAlloc(std.testing.allocator, snapshot_dir, 9, 3);
    defer std.testing.allocator.free(path);
    var file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    var wrong_index: [8]u8 = undefined;
    std.mem.writeInt(u64, &wrong_index, 8, .little);
    try file.writePositionalAll(io, &wrong_index, 16);
    try std.testing.expectError(error.SnapshotPayloadIdentityMismatch, validate(std.testing.allocator, io, snapshot_dir, 9, 3));
    std.mem.writeInt(u64, &wrong_index, 9, .little);
    try file.writePositionalAll(io, &wrong_index, 16);
    try file.writePositionalAll(io, "X", envelope_header_len + 1);
    try std.testing.expectError(error.SnapshotPayloadChecksumMismatch, validate(std.testing.allocator, io, snapshot_dir, 9, 3));
}

test "raft snapshot payload publication rejects an artifact length contract violation" {
    const MisreportedSource = struct {
        bytes: []const u8,

        fn len(self: @This()) u64 {
            return self.bytes.len + 1;
        }

        fn writeTo(self: @This(), writer: *std.Io.Writer) !void {
            try writer.writeAll(self.bytes);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const snapshot_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/artifact-length", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_dir);

    try std.testing.expectError(
        error.SnapshotArtifactSizeMismatch,
        writeSourceAtomically(std.testing.allocator, io, snapshot_dir, 4, 2, MisreportedSource{ .bytes = "short" }),
    );
    const path = try pathAlloc(std.testing.allocator, snapshot_dir, 4, 2);
    defer std.testing.allocator.free(path);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(io, path, .{}));
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
