// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Durable generation manifest for sharded exact-vector blocks.
//!
//! The base generation names every shard. Later generations are sparse deltas
//! and name only touched shards. Readers search the newest applicable delta
//! first, then the base. A bounded delta chain avoids rewriting a multi-GiB
//! f32 base for every source batch while retaining atomic CURRENT publication.

const std = @import("std");
const Allocator = std.mem.Allocator;

const magic: [8]u8 = .{ 'A', 'F', 'V', 'B', 'M', 'A', 'N', 0 };
const legacy_version: u16 = 1;
const version: u16 = 2;
const legacy_header_size: usize = 68;
const header_size: usize = 72;
const segment_size: usize = 40;
const coverage_size: usize = 32;
const footer_size: usize = 12;
pub const max_shards: u32 = 1024;
/// Ordinary online mutation chains stay short so point lookups retain bounded
/// fan-out. An empty bootstrap base may temporarily publish a longer append-
/// only chain: first-load checkpoints contain disjoint corpus slices, and
/// repeatedly coalescing the growing slice rewrites O(n^2) vector bytes before
/// the stable-tip base publication.
pub const max_online_delta_generations: usize = 8;
pub const max_bootstrap_delta_generations: usize = 24;
/// Keep the format admission bound separate from checkpoint policy. This lets
/// a newer binary tighten residency without making a previously published,
/// valid 25--64-run bootstrap manifest unreadable during rolling upgrade or
/// crash recovery.
pub const max_supported_delta_generations: usize = 64;
pub const max_delta_generations: usize = max_supported_delta_generations;
pub const max_segments: usize = @as(usize, max_shards) * (max_delta_generations + 1);
pub const max_coverages: usize = 4096;

pub const Segment = struct {
    generation: u64,
    covered_source_sequence: u64,
    shard_id: u32,
    bytes: u64,
    admission_checksum: u32,
};

/// Complete-base membership certificate for one logical embedding artifact
/// scope. Payload blocks remain shared table-wide; this certificate lets each
/// dense index validate only the artifact family it owns without double
/// counting a family shared by several indexes. Two independent commutative
/// digests make accidental membership substitution observable while keeping
/// compaction streaming and constant-memory.
pub const Coverage = struct {
    scope_hash: u64,
    vector_count: u64,
    key_hash_xor: u64,
    key_hash_sum: u64,
};

pub const Manifest = struct {
    base_generation: u64,
    latest_generation: u64,
    wal_generation: u64,
    wal_committed_bytes: u64,
    covered_source_sequence: u64,
    shard_count: u32,
    segments: []const Segment,
    coverages: []const Coverage = &.{},

    pub fn encodeAlloc(self: Manifest, alloc: Allocator) ![]u8 {
        try self.validate();
        const entries_len = std.math.mul(usize, self.segments.len, segment_size) catch return error.VectorBlockManifestTooLarge;
        const coverage_len = std.math.mul(usize, self.coverages.len, coverage_size) catch return error.VectorBlockManifestTooLarge;
        const body_len = std.math.add(usize, entries_len, coverage_len) catch return error.VectorBlockManifestTooLarge;
        const total_len = std.math.add(usize, header_size + footer_size, body_len) catch return error.VectorBlockManifestTooLarge;
        const out = try alloc.alloc(u8, total_len);
        errdefer alloc.free(out);
        @memcpy(out[0..8], &magic);
        writeU16(out[8..10], version);
        writeU16(out[10..12], 0);
        writeU64(out[12..20], self.base_generation);
        writeU64(out[20..28], self.latest_generation);
        writeU64(out[28..36], self.wal_generation);
        writeU64(out[36..44], self.wal_committed_bytes);
        writeU64(out[44..52], self.covered_source_sequence);
        writeU32(out[52..56], self.shard_count);
        writeU32(out[56..60], @intCast(self.segments.len));
        writeU32(out[60..64], @intCast(self.coverages.len));
        writeU32(out[64..68], 0);
        writeU32(out[68..72], std.hash.Crc32.hash(out[0..68]));
        var pos = header_size;
        for (self.segments) |segment| {
            writeU64(out[pos..][0..8], segment.generation);
            writeU64(out[pos + 8 ..][0..8], segment.covered_source_sequence);
            writeU32(out[pos + 16 ..][0..4], segment.shard_id);
            writeU32(out[pos + 20 ..][0..4], 0);
            writeU64(out[pos + 24 ..][0..8], segment.bytes);
            writeU32(out[pos + 32 ..][0..4], segment.admission_checksum);
            writeU32(out[pos + 36 ..][0..4], 0);
            pos += segment_size;
        }
        for (self.coverages) |coverage| {
            writeU64(out[pos..][0..8], coverage.scope_hash);
            writeU64(out[pos + 8 ..][0..8], coverage.vector_count);
            writeU64(out[pos + 16 ..][0..8], coverage.key_hash_xor);
            writeU64(out[pos + 24 ..][0..8], coverage.key_hash_sum);
            pos += coverage_size;
        }
        writeU32(out[pos..][0..4], std.hash.Crc32.hash(out[0..pos]));
        @memcpy(out[pos + 4 ..][0..8], &magic);
        return out;
    }

    pub fn validate(self: Manifest) !void {
        if (self.base_generation == 0 or self.latest_generation < self.base_generation or self.wal_generation == 0) return error.InvalidVectorBlockManifest;
        if (self.shard_count == 0 or self.shard_count > max_shards or !std.math.isPowerOfTwo(self.shard_count)) return error.InvalidVectorBlockManifest;
        if (self.segments.len < self.shard_count or self.segments.len > max_segments) return error.InvalidVectorBlockManifest;
        if (self.coverages.len > max_coverages) return error.VectorBlockManifestTooLarge;
        var previous_scope: ?u64 = null;
        for (self.coverages) |coverage| {
            if (previous_scope) |previous| if (coverage.scope_hash <= previous) return error.InvalidVectorBlockManifest;
            if (coverage.vector_count == 0 and (coverage.key_hash_xor != 0 or coverage.key_hash_sum != 0)) return error.InvalidVectorBlockManifest;
            previous_scope = coverage.scope_hash;
        }
        var previous_generation: u64 = 0;
        var previous_shard: u32 = 0;
        var generation_coverage: u64 = 0;
        var distinct_generations: usize = 0;
        for (self.segments, 0..) |segment, index| {
            if (segment.generation < self.base_generation or segment.generation > self.latest_generation or
                segment.covered_source_sequence > self.covered_source_sequence or segment.shard_id >= self.shard_count or
                segment.bytes == 0)
            {
                return error.InvalidVectorBlockManifest;
            }
            if (index == 0 or segment.generation != previous_generation) {
                if (index != 0 and segment.generation <= previous_generation) return error.InvalidVectorBlockManifest;
                distinct_generations += 1;
                generation_coverage = segment.covered_source_sequence;
            } else {
                if (segment.shard_id <= previous_shard or segment.covered_source_sequence != generation_coverage) return error.InvalidVectorBlockManifest;
            }
            if (segment.generation == self.base_generation) {
                const expected_shard: u32 = @intCast(index);
                if (index >= self.shard_count or segment.shard_id != expected_shard) return error.InvalidVectorBlockManifest;
            } else if (index < self.shard_count) {
                return error.InvalidVectorBlockManifest;
            }
            previous_generation = segment.generation;
            previous_shard = segment.shard_id;
        }
        if (previous_generation != self.latest_generation or distinct_generations == 0 or distinct_generations - 1 > max_delta_generations) return error.InvalidVectorBlockManifest;
        if (self.segments[self.shard_count - 1].generation != self.base_generation) return error.InvalidVectorBlockManifest;
    }
};

pub const Decoded = struct {
    manifest: Manifest,
    owned_segments: []Segment,
    owned_coverages: []Coverage,

    pub fn deinit(self: *Decoded, alloc: Allocator) void {
        alloc.free(self.owned_segments);
        alloc.free(self.owned_coverages);
        self.* = undefined;
    }
};

pub fn decodeAlloc(alloc: Allocator, bytes: []const u8) !Decoded {
    if (bytes.len < legacy_header_size + footer_size or !std.mem.eql(u8, bytes[0..8], &magic)) return error.InvalidVectorBlockManifest;
    const decoded_version = readU16(bytes[8..10]);
    if (decoded_version != legacy_version and decoded_version != version) return error.UnsupportedVectorBlockManifestVersion;
    if (readU16(bytes[10..12]) != 0) return error.UnsupportedVectorBlockManifestFlags;
    const decoded_header_size = if (decoded_version == legacy_version) legacy_header_size else header_size;
    if (bytes.len < decoded_header_size + footer_size) return error.InvalidVectorBlockManifest;
    if (decoded_version == legacy_version) {
        if (readU32(bytes[60..64]) != 0) return error.UnsupportedVectorBlockManifestFlags;
        if (readU32(bytes[64..68]) != std.hash.Crc32.hash(bytes[0..64])) return error.VectorBlockManifestChecksumMismatch;
    } else {
        if (readU32(bytes[64..68]) != 0) return error.UnsupportedVectorBlockManifestFlags;
        if (readU32(bytes[68..72]) != std.hash.Crc32.hash(bytes[0..68])) return error.VectorBlockManifestChecksumMismatch;
    }
    const segment_count: usize = @intCast(readU32(bytes[56..60]));
    const coverage_count: usize = if (decoded_version == legacy_version) 0 else @intCast(readU32(bytes[60..64]));
    if (segment_count > max_segments) return error.VectorBlockManifestTooLarge;
    if (coverage_count > max_coverages) return error.VectorBlockManifestTooLarge;
    const entries_len = std.math.mul(usize, segment_count, segment_size) catch return error.InvalidVectorBlockManifest;
    const coverage_len = std.math.mul(usize, coverage_count, coverage_size) catch return error.InvalidVectorBlockManifest;
    const body_len = std.math.add(usize, entries_len, coverage_len) catch return error.InvalidVectorBlockManifest;
    const expected_len = std.math.add(usize, decoded_header_size + footer_size, body_len) catch return error.InvalidVectorBlockManifest;
    if (bytes.len != expected_len) return error.InvalidVectorBlockManifest;
    const footer = bytes[bytes.len - footer_size ..];
    if (!std.mem.eql(u8, footer[4..12], &magic)) return error.InvalidVectorBlockManifest;
    if (readU32(footer[0..4]) != std.hash.Crc32.hash(bytes[0 .. bytes.len - footer_size])) return error.VectorBlockManifestChecksumMismatch;

    const segments = try alloc.alloc(Segment, segment_count);
    errdefer alloc.free(segments);
    var pos = decoded_header_size;
    for (segments) |*segment| {
        if (readU32(bytes[pos + 20 ..][0..4]) != 0 or readU32(bytes[pos + 36 ..][0..4]) != 0) return error.UnsupportedVectorBlockManifestFlags;
        segment.* = .{
            .generation = readU64(bytes[pos..][0..8]),
            .covered_source_sequence = readU64(bytes[pos + 8 ..][0..8]),
            .shard_id = readU32(bytes[pos + 16 ..][0..4]),
            .bytes = readU64(bytes[pos + 24 ..][0..8]),
            .admission_checksum = readU32(bytes[pos + 32 ..][0..4]),
        };
        pos += segment_size;
    }
    const coverages = try alloc.alloc(Coverage, coverage_count);
    errdefer alloc.free(coverages);
    for (coverages) |*coverage| {
        coverage.* = .{
            .scope_hash = readU64(bytes[pos..][0..8]),
            .vector_count = readU64(bytes[pos + 8 ..][0..8]),
            .key_hash_xor = readU64(bytes[pos + 16 ..][0..8]),
            .key_hash_sum = readU64(bytes[pos + 24 ..][0..8]),
        };
        pos += coverage_size;
    }
    const manifest: Manifest = .{
        .base_generation = readU64(bytes[12..20]),
        .latest_generation = readU64(bytes[20..28]),
        .wal_generation = readU64(bytes[28..36]),
        .wal_committed_bytes = readU64(bytes[36..44]),
        .covered_source_sequence = readU64(bytes[44..52]),
        .shard_count = readU32(bytes[52..56]),
        .segments = segments,
        .coverages = coverages,
    };
    try manifest.validate();
    return .{ .manifest = manifest, .owned_segments = segments, .owned_coverages = coverages };
}

fn writeU16(out: []u8, value: u16) void {
    std.mem.writeInt(u16, out[0..2], value, .big);
}
fn writeU32(out: []u8, value: u32) void {
    std.mem.writeInt(u32, out[0..4], value, .big);
}
fn writeU64(out: []u8, value: u64) void {
    std.mem.writeInt(u64, out[0..8], value, .big);
}
fn readU16(bytes: []const u8) u16 {
    return std.mem.readInt(u16, bytes[0..2], .big);
}
fn readU32(bytes: []const u8) u32 {
    return std.mem.readInt(u32, bytes[0..4], .big);
}
fn readU64(bytes: []const u8) u64 {
    return std.mem.readInt(u64, bytes[0..8], .big);
}

test "vector block manifest round trips a sparse delta chain" {
    const alloc = std.testing.allocator;
    const segments = [_]Segment{
        .{ .generation = 1, .covered_source_sequence = 10, .shard_id = 0, .bytes = 100, .admission_checksum = 1 },
        .{ .generation = 1, .covered_source_sequence = 10, .shard_id = 1, .bytes = 101, .admission_checksum = 2 },
        .{ .generation = 1, .covered_source_sequence = 10, .shard_id = 2, .bytes = 102, .admission_checksum = 3 },
        .{ .generation = 1, .covered_source_sequence = 10, .shard_id = 3, .bytes = 103, .admission_checksum = 4 },
        .{ .generation = 2, .covered_source_sequence = 20, .shard_id = 1, .bytes = 20, .admission_checksum = 5 },
        .{ .generation = 2, .covered_source_sequence = 20, .shard_id = 3, .bytes = 21, .admission_checksum = 6 },
    };
    const coverages = [_]Coverage{
        .{ .scope_hash = 7, .vector_count = 2, .key_hash_xor = 11, .key_hash_sum = 23 },
        .{ .scope_hash = 9, .vector_count = 1, .key_hash_xor = 17, .key_hash_sum = 17 },
    };
    const source: Manifest = .{
        .base_generation = 1,
        .latest_generation = 2,
        .wal_generation = 3,
        .wal_committed_bytes = 42,
        .covered_source_sequence = 21,
        .shard_count = 4,
        .segments = &segments,
        .coverages = &coverages,
    };
    const encoded = try source.encodeAlloc(alloc);
    defer alloc.free(encoded);
    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), decoded.manifest.latest_generation);
    try std.testing.expectEqual(@as(usize, 6), decoded.manifest.segments.len);
    try std.testing.expectEqual(@as(u32, 3), decoded.manifest.segments[5].shard_id);
    try std.testing.expectEqualSlices(Coverage, &coverages, decoded.manifest.coverages);
}

test "vector block manifest requires every base shard" {
    const segments = [_]Segment{
        .{ .generation = 1, .covered_source_sequence = 1, .shard_id = 0, .bytes = 1, .admission_checksum = 1 },
        .{ .generation = 1, .covered_source_sequence = 1, .shard_id = 2, .bytes = 1, .admission_checksum = 1 },
    };
    const invalid: Manifest = .{
        .base_generation = 1,
        .latest_generation = 1,
        .wal_generation = 1,
        .wal_committed_bytes = 0,
        .covered_source_sequence = 1,
        .shard_count = 2,
        .segments = &segments,
    };
    try std.testing.expectError(error.InvalidVectorBlockManifest, invalid.validate());
}

test "vector block manifest keeps version one generations readable" {
    const alloc = std.testing.allocator;
    const total_len = legacy_header_size + segment_size + footer_size;
    const encoded = try alloc.alloc(u8, total_len);
    defer alloc.free(encoded);
    @memcpy(encoded[0..8], &magic);
    writeU16(encoded[8..10], legacy_version);
    writeU16(encoded[10..12], 0);
    writeU64(encoded[12..20], 1);
    writeU64(encoded[20..28], 1);
    writeU64(encoded[28..36], 2);
    writeU64(encoded[36..44], 0);
    writeU64(encoded[44..52], 9);
    writeU32(encoded[52..56], 1);
    writeU32(encoded[56..60], 1);
    writeU32(encoded[60..64], 0);
    writeU32(encoded[64..68], std.hash.Crc32.hash(encoded[0..64]));
    var pos: usize = legacy_header_size;
    writeU64(encoded[pos..][0..8], 1);
    writeU64(encoded[pos + 8 ..][0..8], 9);
    writeU32(encoded[pos + 16 ..][0..4], 0);
    writeU32(encoded[pos + 20 ..][0..4], 0);
    writeU64(encoded[pos + 24 ..][0..8], 128);
    writeU32(encoded[pos + 32 ..][0..4], 77);
    writeU32(encoded[pos + 36 ..][0..4], 0);
    pos += segment_size;
    writeU32(encoded[pos..][0..4], std.hash.Crc32.hash(encoded[0..pos]));
    @memcpy(encoded[pos + 4 ..][0..8], &magic);

    var decoded = try decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), decoded.manifest.segments.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.manifest.coverages.len);
}
