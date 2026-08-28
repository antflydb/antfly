// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Transactional WAL for the table-level exact-vector projection.
//!
//! A source batch is replayable only after its commit frame. Complete and
//! partial uncommitted tails are ignored; corruption in a complete frame
//! fails closed. Artifact keys and f32 payloads are covered by the frame CRC.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const vector_block = @import("vector_block.zig");

const magic: u32 = 0x41465657; // AFVW
const version: u16 = 1;
const frame_header_len: usize = 64;
const max_frame_len: usize = 256 * 1024 * 1024;

pub const Kind = enum(u8) {
    upsert = 1,
    tombstone = 2,
    coverage = 3,
    commit = 255,
};

pub const Record = struct {
    kind: Kind,
    batch_id: u64,
    source_sequence: u64,
    revision: u64,
    key_hash: u64,
    key: []const u8,
    dims: u32,
    vector_bytes: []const u8,

    pub fn vectorView(self: Record) !?[]const f32 {
        if (self.kind != .upsert) return error.VectorWalRecordHasNoVector;
        if (builtin.target.cpu.arch.endian() != .little) return null;
        if (@intFromPtr(self.vector_bytes.ptr) % @alignOf(f32) != 0) return null;
        const aligned: []align(@alignOf(f32)) const u8 = @alignCast(self.vector_bytes);
        return std.mem.bytesAsSlice(f32, aligned);
    }

    pub fn decodeVectorInto(self: Record, out: []f32) ![]const f32 {
        if (self.kind != .upsert) return error.VectorWalRecordHasNoVector;
        if (self.dims > out.len) return error.BufferTooSmall;
        if (try self.vectorView()) |view| {
            @memcpy(out[0..self.dims], view);
            return out[0..self.dims];
        }
        var pos: usize = 0;
        for (out[0..self.dims]) |*value| {
            value.* = @bitCast(std.mem.readInt(u32, self.vector_bytes[pos..][0..4], .little));
            pos += 4;
        }
        return out[0..self.dims];
    }
};

pub const Writer = struct {
    alloc: Allocator,
    out: std.ArrayListUnmanaged(u8) = .empty,
    open_batch: ?u64 = null,
    open_batch_records: usize = 0,
    open_batch_max_sequence: u64 = 0,
    last_committed_batch: ?u64 = null,
    covered_source_sequence: u64 = 0,

    pub fn init(alloc: Allocator) Writer {
        return .{ .alloc = alloc };
    }

    pub fn initAfterCommitted(alloc: Allocator, last_committed_batch: ?u64, covered_source_sequence: u64) Writer {
        return .{
            .alloc = alloc,
            .last_committed_batch = last_committed_batch,
            .covered_source_sequence = covered_source_sequence,
        };
    }

    pub fn deinit(self: *Writer) void {
        self.out.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn bytes(self: *const Writer) []const u8 {
        return self.out.items;
    }

    pub fn appendUpsert(
        self: *Writer,
        batch_id: u64,
        source_sequence: u64,
        revision: u64,
        key: []const u8,
        vector: []const f32,
    ) !void {
        if (key.len == 0) return error.InvalidVectorWalKey;
        if (vector.len == 0) return error.InvalidVectorDimensions;
        const dims = std.math.cast(u32, vector.len) orelse return error.VectorWalRecordTooLarge;
        const vector_bytes_len = std.math.mul(usize, vector.len, @sizeOf(f32)) catch return error.VectorWalRecordTooLarge;
        const payload_len = std.math.add(usize, key.len, vector_bytes_len) catch return error.VectorWalRecordTooLarge;
        const payload = try self.alloc.alloc(u8, payload_len);
        defer self.alloc.free(payload);
        @memcpy(payload[0..key.len], key);
        var pos = key.len;
        for (vector) |value| {
            std.mem.writeInt(u32, payload[pos..][0..4], @bitCast(value), .little);
            pos += 4;
        }
        try self.append(.{
            .kind = .upsert,
            .batch_id = batch_id,
            .source_sequence = source_sequence,
            .revision = revision,
            .key_hash = vector_block.keyHash(key),
            .key = payload[0..key.len],
            .dims = dims,
            .vector_bytes = payload[key.len..],
        });
    }

    pub fn appendTombstone(self: *Writer, batch_id: u64, source_sequence: u64, revision: u64, key: []const u8) !void {
        if (key.len == 0) return error.InvalidVectorWalKey;
        try self.append(.{
            .kind = .tombstone,
            .batch_id = batch_id,
            .source_sequence = source_sequence,
            .revision = revision,
            .key_hash = vector_block.keyHash(key),
            .key = key,
            .dims = 0,
            .vector_bytes = &.{},
        });
    }

    pub fn appendCoverage(self: *Writer, batch_id: u64, source_sequence: u64) !void {
        try self.append(.{
            .kind = .coverage,
            .batch_id = batch_id,
            .source_sequence = source_sequence,
            .revision = 0,
            .key_hash = 0,
            .key = &.{},
            .dims = 0,
            .vector_bytes = &.{},
        });
    }

    fn append(self: *Writer, record: Record) !void {
        const starts_batch = self.open_batch == null;
        if (self.open_batch) |open_batch| {
            if (record.batch_id != open_batch) return error.InterleavedVectorWalBatch;
            if (record.source_sequence < self.open_batch_max_sequence) return error.OutOfOrderVectorWalSequence;
        } else {
            if (self.last_committed_batch) |last| if (record.batch_id <= last) return error.OutOfOrderVectorWalBatch;
            if (record.source_sequence < self.covered_source_sequence) return error.OutOfOrderVectorWalSequence;
        }
        try appendFrame(self.alloc, &self.out, record);
        if (starts_batch) self.open_batch = record.batch_id;
        self.open_batch_records += 1;
        self.open_batch_max_sequence = @max(self.open_batch_max_sequence, record.source_sequence);
    }

    pub fn commit(self: *Writer, batch_id: u64, covered_source_sequence: u64) !void {
        if (self.open_batch == null or self.open_batch.? != batch_id or self.open_batch_records == 0) return error.InvalidVectorWalCommit;
        if (covered_source_sequence < self.open_batch_max_sequence) return error.InvalidVectorWalCommit;
        try appendFrame(self.alloc, &self.out, .{
            .kind = .commit,
            .batch_id = batch_id,
            .source_sequence = covered_source_sequence,
            .revision = 0,
            .key_hash = 0,
            .key = &.{},
            .dims = 0,
            .vector_bytes = &.{},
        });
        self.last_committed_batch = batch_id;
        self.covered_source_sequence = covered_source_sequence;
        self.open_batch = null;
        self.open_batch_records = 0;
        self.open_batch_max_sequence = 0;
    }
};

pub const Replay = struct {
    alloc: Allocator,
    records: std.ArrayListUnmanaged(Record) = .empty,
    valid_bytes: usize = 0,
    committed_bytes: usize = 0,
    last_committed_batch: ?u64 = null,
    covered_source_sequence: u64 = 0,

    pub fn parse(alloc: Allocator, bytes: []const u8) !Replay {
        var replay: Replay = .{ .alloc = alloc };
        errdefer replay.deinit();
        var offset: usize = 0;
        var open_batch: ?u64 = null;
        var open_batch_start: usize = 0;
        var open_batch_records: usize = 0;
        var open_batch_max_sequence: u64 = 0;
        while (offset < bytes.len) {
            const decoded = (try decodeFrame(bytes[offset..])) orelse break;
            const record = decoded.record;
            const next_offset = std.math.add(usize, offset, decoded.frame_len) catch return error.CorruptedVectorWal;
            if (record.kind == .commit) {
                if (open_batch == null or open_batch.? != record.batch_id or open_batch_records == 0) return error.InvalidVectorWalCommit;
                if (record.source_sequence < open_batch_max_sequence) return error.InvalidVectorWalCommit;
                replay.last_committed_batch = record.batch_id;
                replay.covered_source_sequence = record.source_sequence;
                replay.committed_bytes = next_offset;
                open_batch = null;
                open_batch_records = 0;
                open_batch_max_sequence = 0;
            } else {
                if (open_batch) |batch_id| {
                    if (record.batch_id != batch_id) return error.InterleavedVectorWalBatch;
                    if (record.source_sequence < open_batch_max_sequence) return error.OutOfOrderVectorWalSequence;
                } else {
                    if (replay.last_committed_batch) |last| if (record.batch_id <= last) return error.OutOfOrderVectorWalBatch;
                    if (record.source_sequence < replay.covered_source_sequence) return error.OutOfOrderVectorWalSequence;
                    open_batch = record.batch_id;
                    open_batch_start = replay.records.items.len;
                }
                try replay.records.append(alloc, record);
                open_batch_records += 1;
                open_batch_max_sequence = @max(open_batch_max_sequence, record.source_sequence);
            }
            offset = next_offset;
            replay.valid_bytes = offset;
        }
        if (open_batch != null) replay.records.shrinkRetainingCapacity(open_batch_start);
        return replay;
    }

    pub fn deinit(self: *Replay) void {
        self.records.deinit(self.alloc);
        self.* = undefined;
    }
};

const DecodedFrame = struct {
    record: Record,
    frame_len: usize,
};

fn appendFrame(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), record: Record) !void {
    try validateRecord(record);
    const padded_key_len = std.mem.alignForward(usize, record.key.len, @alignOf(f32));
    const payload_len = std.math.add(usize, padded_key_len, record.vector_bytes.len) catch return error.VectorWalRecordTooLarge;
    const total_len = std.math.add(usize, frame_header_len, payload_len) catch return error.VectorWalRecordTooLarge;
    if (total_len > max_frame_len or total_len > std.math.maxInt(u32)) return error.VectorWalRecordTooLarge;
    const start = out.items.len;
    try out.resize(alloc, start + total_len);
    const frame = out.items[start..][0..total_len];
    std.mem.writeInt(u32, frame[0..4], magic, .big);
    std.mem.writeInt(u16, frame[4..6], version, .big);
    std.mem.writeInt(u16, frame[6..8], frame_header_len, .big);
    std.mem.writeInt(u32, frame[8..12], @intCast(total_len), .big);
    @memset(frame[12..16], 0);
    frame[16] = @intFromEnum(record.kind);
    @memset(frame[17..24], 0);
    std.mem.writeInt(u64, frame[24..32], record.batch_id, .big);
    std.mem.writeInt(u64, frame[32..40], record.source_sequence, .big);
    std.mem.writeInt(u64, frame[40..48], record.revision, .big);
    std.mem.writeInt(u64, frame[48..56], record.key_hash, .big);
    std.mem.writeInt(u32, frame[56..60], @intCast(record.key.len), .big);
    std.mem.writeInt(u32, frame[60..64], record.dims, .big);
    @memcpy(frame[64..][0..record.key.len], record.key);
    @memset(frame[64 + record.key.len ..][0 .. padded_key_len - record.key.len], 0);
    @memcpy(frame[64 + padded_key_len ..], record.vector_bytes);
    std.mem.writeInt(u32, frame[12..16], std.hash.Crc32.hash(frame[16..]), .big);
}

fn decodeFrame(bytes: []const u8) !?DecodedFrame {
    if (bytes.len < 16) return null;
    if (std.mem.readInt(u32, bytes[0..4], .big) != magic) return error.BadVectorWalMagic;
    if (std.mem.readInt(u16, bytes[4..6], .big) != version) return error.UnsupportedVectorWalVersion;
    if (std.mem.readInt(u16, bytes[6..8], .big) != frame_header_len) return error.UnsupportedVectorWalHeader;
    const total_len: usize = @intCast(std.mem.readInt(u32, bytes[8..12], .big));
    if (total_len < frame_header_len or total_len > max_frame_len) return error.CorruptedVectorWal;
    if (bytes.len < total_len) return null;
    const frame = bytes[0..total_len];
    if (std.mem.readInt(u32, frame[12..16], .big) != std.hash.Crc32.hash(frame[16..])) return error.VectorWalChecksumMismatch;
    for (frame[17..24]) |reserved| if (reserved != 0) return error.UnsupportedVectorWalFlags;
    const kind: Kind = switch (frame[16]) {
        @intFromEnum(Kind.upsert) => .upsert,
        @intFromEnum(Kind.tombstone) => .tombstone,
        @intFromEnum(Kind.coverage) => .coverage,
        @intFromEnum(Kind.commit) => .commit,
        else => return error.InvalidVectorWalRecord,
    };
    const key_len: usize = @intCast(std.mem.readInt(u32, frame[56..60], .big));
    const dims = std.mem.readInt(u32, frame[60..64], .big);
    const vector_len = std.math.mul(usize, dims, @sizeOf(f32)) catch return error.CorruptedVectorWal;
    const padded_key_len = std.mem.alignForward(usize, key_len, @alignOf(f32));
    if (padded_key_len > total_len - frame_header_len or vector_len != total_len - frame_header_len - padded_key_len) return error.CorruptedVectorWal;
    if (!std.mem.allEqual(u8, frame[64 + key_len ..][0 .. padded_key_len - key_len], 0)) return error.CorruptedVectorWal;
    const record: Record = .{
        .kind = kind,
        .batch_id = std.mem.readInt(u64, frame[24..32], .big),
        .source_sequence = std.mem.readInt(u64, frame[32..40], .big),
        .revision = std.mem.readInt(u64, frame[40..48], .big),
        .key_hash = std.mem.readInt(u64, frame[48..56], .big),
        .key = frame[64..][0..key_len],
        .dims = dims,
        .vector_bytes = frame[64 + padded_key_len ..],
    };
    try validateRecord(record);
    return .{ .record = record, .frame_len = total_len };
}

fn validateRecord(record: Record) !void {
    switch (record.kind) {
        .upsert => {
            if (record.key.len == 0 or record.dims == 0) return error.InvalidVectorWalRecord;
            if (record.key_hash != vector_block.keyHash(record.key)) return error.InvalidVectorWalRecord;
            const vector_len = std.math.mul(usize, record.dims, @sizeOf(f32)) catch return error.InvalidVectorWalRecord;
            if (record.vector_bytes.len != vector_len) return error.InvalidVectorWalRecord;
        },
        .tombstone => {
            if (record.key.len == 0 or record.key_hash != vector_block.keyHash(record.key) or record.dims != 0 or record.vector_bytes.len != 0) return error.InvalidVectorWalRecord;
        },
        .coverage, .commit => {
            if (record.revision != 0 or record.key_hash != 0 or record.key.len != 0 or record.dims != 0 or record.vector_bytes.len != 0) return error.InvalidVectorWalRecord;
        },
    }
}

test "vector WAL ignores an uncommitted tail" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendUpsert(1, 10, 1, "artifact-a", &.{ 1.0, 2.0 });
    try writer.commit(1, 10);
    const committed_len = writer.bytes().len;
    try writer.appendTombstone(2, 11, 2, "artifact-a");

    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqual(committed_len, replay.committed_bytes);
    try std.testing.expectEqual(@as(usize, 1), replay.records.items.len);
    try std.testing.expectEqual(@as(u64, 10), replay.covered_source_sequence);
    const values = (try replay.records.items[0].vectorView()).?;
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, values);
}

test "vector WAL commits coverage-only source batches" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendCoverage(7, 42);
    try writer.commit(7, 42);
    var replay = try Replay.parse(alloc, writer.bytes());
    defer replay.deinit();
    try std.testing.expectEqual(@as(u64, 42), replay.covered_source_sequence);
    try std.testing.expectEqual(Kind.coverage, replay.records.items[0].kind);
}

test "vector WAL rejects corruption in complete frames" {
    const alloc = std.testing.allocator;
    var writer = Writer.init(alloc);
    defer writer.deinit();
    try writer.appendUpsert(1, 1, 1, "artifact-a", &.{1.0});
    try writer.commit(1, 1);
    writer.out.items[writer.out.items.len - 1] ^= 0xff;
    try std.testing.expectError(error.VectorWalChecksumMismatch, Replay.parse(alloc, writer.bytes()));
}
