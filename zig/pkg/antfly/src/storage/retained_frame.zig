// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Bounded random access to a retained logical frame. REF5's descriptor is
//! small and authenticated; immutable chunks may live in DocStore or a spool.
//! No caller must materialize or rehash the complete after-image to resume.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const Digest = [32]u8;
pub const chunk_bytes: u32 = 1024 * 1024;
pub const max_logical_bytes: u32 = 256 * 1024 * 1024;
pub const max_effects: u32 = 65536;
pub const max_chunks: u32 = max_logical_bytes / chunk_bytes;
const magic = "RFV5";
const descriptor_domain = "antfly:retained-frame:REF5:";
const header_bytes = 92;

fn descriptorDigest(raw: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(descriptor_domain);
    hash.update(raw);
    return hash.finalResult();
}

pub const Source = struct {
    context: *anyopaque,
    read_chunk: *const fn (*anyopaque, u64, u32, []u8) anyerror!usize,
    /// A local disposable spool can request retransfer on physical damage;
    /// durable source chunks retain the terminal corruption classification.
    corruption_error: anyerror = error.RetainedEffectsCorrupt,

    pub fn readChunk(self: Source, sequence: u64, ordinal: u32, out: []u8) !usize {
        return self.read_chunk(self.context, sequence, ordinal, out);
    }
};

pub const Descriptor = struct {
    sequence: u64,
    total: u32,
    direct_vectors: bool,
    graph_artifacts: bool = false,
    logical_digest: Digest,
    payload_checksum: Digest,
    chunk_hashes: []const Digest,
    effect_offsets: []const u32,

    pub fn encodeAlloc(self: Descriptor, alloc: Allocator) ![]u8 {
        const count = try chunkCount(self.total);
        if ((self.graph_artifacts and !self.direct_vectors) or self.chunk_hashes.len != count or self.effect_offsets.len == 0 or
            self.effect_offsets.len > max_effects or self.effect_offsets[0] != 16) return error.RetainedEffectsCorrupt;
        var previous: u32 = 0;
        for (self.effect_offsets) |offset| {
            if (offset <= previous or offset >= self.total - 32) return error.RetainedEffectsCorrupt;
            previous = offset;
        }
        const len = try encodedLength(count, @intCast(self.effect_offsets.len));
        const out = try alloc.alloc(u8, len);
        errdefer alloc.free(out);
        @memcpy(out[0..4], magic);
        std.mem.writeInt(u64, out[4..12], self.sequence, .little);
        std.mem.writeInt(u32, out[12..16], self.total, .little);
        std.mem.writeInt(u32, out[16..20], @intCast(self.effect_offsets.len), .little);
        std.mem.writeInt(u32, out[20..24], count, .little);
        std.mem.writeInt(u32, out[24..28], @as(u32, @intFromBool(self.direct_vectors)) | (@as(u32, @intFromBool(self.graph_artifacts)) << 1), .little);
        @memcpy(out[28..60], &self.logical_digest);
        @memcpy(out[60..92], &self.payload_checksum);
        var pos: usize = header_bytes;
        for (self.chunk_hashes) |hash| {
            @memcpy(out[pos..][0..32], &hash);
            pos += 32;
        }
        for (self.effect_offsets) |offset| {
            std.mem.writeInt(u32, out[pos..][0..4], offset, .little);
            pos += 4;
        }
        @memcpy(out[pos..][0..32], &descriptorDigest(out[0..pos]));
        return out;
    }
};

fn chunkCount(total: u32) !u32 {
    if (total < 49 or total > max_logical_bytes) return error.RetainedEffectsCorrupt;
    return std.math.divCeil(u32, total, chunk_bytes) catch unreachable;
}

fn encodedLength(chunks: u32, effects: u32) !usize {
    if (chunks == 0 or chunks > max_chunks or effects == 0 or effects > max_effects) return error.RetainedEffectsCorrupt;
    return header_bytes + @as(usize, chunks) * 32 + @as(usize, effects) * 4 + 32;
}

pub fn descriptorLength(total: u32, effects: u32) !usize {
    return encodedLength(try chunkCount(total), effects);
}

pub const View = struct {
    descriptor: []const u8,
    source: Source,
    sequence: u64,
    total: u32,
    effect_count: u32,
    chunk_count: u32,
    direct_vectors: bool,
    graph_artifacts: bool,
    logical_digest: Digest,
    /// Domain-separated identity of the descriptor, offset table and every
    /// immutable chunk hash. This, not the logical payload hash, is the wire
    /// identity for REF5 transfer and resume.
    descriptor_digest: Digest,
    payload_checksum: Digest,

    pub const ChunkCache = struct {
        bytes: []u8,
        ordinal: ?u32 = null,
        descriptor_digest: Digest = @splat(0),

        pub fn invalidate(self: *ChunkCache) void {
            self.ordinal = null;
        }
    };

    pub fn init(raw: []const u8, sequence: u64, source: Source, cache: *ChunkCache) !View {
        const view = try fromDescriptor(raw, sequence, source);
        try view.validateEnvelope(cache);
        return view;
    }

    /// Authenticate the bounded manifest without accessing any chunk. A
    /// receiving spool uses this before all chunks exist; it must not expose
    /// effects until validateEnvelope and the accessed chunk checks pass.
    pub fn fromDescriptor(raw: []const u8, sequence: u64, source: Source) !View {
        if (raw.len < header_bytes + 32 or !std.mem.eql(u8, raw[0..4], magic) or
            std.mem.readInt(u64, raw[4..12], .little) != sequence) return error.RetainedEffectsCorrupt;
        const total = std.mem.readInt(u32, raw[12..16], .little);
        const effects = std.mem.readInt(u32, raw[16..20], .little);
        const chunks = std.mem.readInt(u32, raw[20..24], .little);
        const flags = std.mem.readInt(u32, raw[24..28], .little);
        if (flags > 3 or (flags & 2 != 0 and flags & 1 == 0) or chunks != try chunkCount(total) or raw.len != try encodedLength(chunks, effects))
            return error.RetainedEffectsCorrupt;
        const digest = descriptorDigest(raw[0 .. raw.len - 32]);
        if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.RetainedEffectsCorrupt;
        const view: View = .{
            .descriptor = raw,
            .source = source,
            .sequence = sequence,
            .total = total,
            .effect_count = effects,
            .chunk_count = chunks,
            .direct_vectors = flags & 1 != 0,
            .graph_artifacts = flags & 2 != 0,
            .logical_digest = raw[28..60].*,
            .descriptor_digest = digest,
            .payload_checksum = raw[60..92].*,
        };
        if (view.effectOffset(0) != 16) return error.RetainedEffectsCorrupt;
        var previous: u32 = 0;
        for (0..effects) |index| {
            const offset = view.effectOffset(@intCast(index));
            if (offset <= previous or offset >= total - 32) return error.RetainedEffectsCorrupt;
            previous = offset;
        }
        return view;
    }

    pub fn validateEnvelope(self: View, cache: *ChunkCache) !void {
        var header: [16]u8 = undefined;
        if (try self.readAt(0, &header, cache) != header.len or
            (!std.mem.eql(u8, header[0..4], if (self.graph_artifacts) "REFG" else if (self.direct_vectors) "REF4" else "REF3")) or
            std.mem.readInt(u64, header[4..12], .little) != self.sequence or
            std.mem.readInt(u32, header[12..16], .little) != self.effect_count) return error.RetainedEffectsCorrupt;
        var trailer: [32]u8 = undefined;
        if (try self.readAt(self.total - 32, &trailer, cache) != trailer.len or
            !std.mem.eql(u8, &trailer, &self.payload_checksum)) return error.RetainedEffectsCorrupt;
    }

    fn chunkHash(self: View, ordinal: u32) Digest {
        const pos = header_bytes + @as(usize, ordinal) * 32;
        return self.descriptor[pos..][0..32].*;
    }

    pub fn chunkHashAt(self: View, ordinal: u32) !Digest {
        if (ordinal >= self.chunk_count) return error.RetainedEffectsCursorMismatch;
        return self.chunkHash(ordinal);
    }

    fn effectOffset(self: View, index: u32) u32 {
        const pos = header_bytes + @as(usize, self.chunk_count) * 32 + @as(usize, index) * 4;
        return std.mem.readInt(u32, self.descriptor[pos..][0..4], .little);
    }

    pub fn offsetAt(self: View, index: u32) !u32 {
        if (index == self.effect_count) return self.total - 32;
        if (index > self.effect_count) return error.RetainedEffectsCursorMismatch;
        return self.effectOffset(index);
    }

    /// Every accessed chunk is checked before any of its bytes are exposed.
    /// The caller keeps this cache across effects/pages so sequential reads
    /// verify each 1 MiB chunk once, not once per small header or RPC.
    /// `out` must not alias `cache.bytes`.
    pub fn readAt(self: View, offset: u32, out: []u8, cache: *ChunkCache) !usize {
        if (offset > self.total or cache.bytes.len < chunk_bytes) return error.RetainedEffectsCorrupt;
        const remaining = @min(out.len, @as(usize, self.total - offset));
        var copied: usize = 0;
        while (copied < remaining) {
            const position = @as(u64, offset) + copied;
            const ordinal: u32 = @intCast(position / chunk_bytes);
            const chunk_start = @as(u64, ordinal) * chunk_bytes;
            const chunk_len: usize = @intCast(@min(chunk_bytes, @as(u64, self.total) - chunk_start));
            if (cache.ordinal != ordinal or !std.mem.eql(u8, &cache.descriptor_digest, &self.descriptor_digest)) {
                cache.invalidate();
                if (try self.source.readChunk(self.sequence, ordinal, cache.bytes[0..chunk_len]) != chunk_len)
                    return self.source.corruption_error;
                var digest: Digest = undefined;
                std.crypto.hash.sha2.Sha256.hash(cache.bytes[0..chunk_len], &digest, .{});
                if (!std.mem.eql(u8, &digest, &self.chunkHash(ordinal))) return self.source.corruption_error;
                cache.ordinal = ordinal;
                cache.descriptor_digest = self.descriptor_digest;
            }
            const within: usize = @intCast(position - chunk_start);
            const n = @min(chunk_len - within, remaining - copied);
            @memcpy(out[copied..][0..n], cache.bytes[within..][0..n]);
            copied += n;
        }
        return copied;
    }

    pub const EffectHeader = struct {
        key_offset: u32,
        key_len: u32,
        value_offset: u32,
        value_len: ?u32,
        timestamp: u64,
        next_offset: u32,
    };

    /// The offset table is untrusted until each entry's exact end is checked.
    /// Key-family and value-schema validation belong to the consumer before
    /// it applies the effect, after reading the authenticated bytes.
    pub fn effectAt(self: View, index: u32, cache: *ChunkCache) !EffectHeader {
        if (index >= self.effect_count) return error.RetainedEffectsCursorMismatch;
        const start = self.effectOffset(index);
        const end = if (index + 1 < self.effect_count) self.effectOffset(index + 1) else self.total - 32;
        if (end <= start or end - start < 17) return error.RetainedEffectsCorrupt;
        var header: [16]u8 = undefined;
        if (try self.readAt(start, &header, cache) != header.len) return error.RetainedEffectsCorrupt;
        const key_len = std.mem.readInt(u32, header[0..4], .little);
        const raw_value_len = std.mem.readInt(u32, header[4..8], .little);
        const timestamp = std.mem.readInt(u64, header[8..16], .little);
        const value_len: ?u32 = if (raw_value_len == std.math.maxInt(u32)) null else raw_value_len;
        const required = @as(u64, 16) + key_len + (value_len orelse 0);
        if (key_len == 0 or required != end - start or (value_len == null and timestamp != 0))
            return error.RetainedEffectsCorrupt;
        return .{ .key_offset = start + 16, .key_len = key_len, .value_offset = start + 16 + key_len, .value_len = value_len, .timestamp = timestamp, .next_offset = end };
    }
};

test "REF5 descriptor authenticates point chunks and exact effect offsets" {
    const alloc = std.testing.allocator;
    var bytes: [66]u8 = undefined;
    @memcpy(bytes[0..4], "REF3");
    std.mem.writeInt(u64, bytes[4..12], 7, .little);
    std.mem.writeInt(u32, bytes[12..16], 1, .little);
    std.mem.writeInt(u32, bytes[16..20], 1, .little);
    std.mem.writeInt(u32, bytes[20..24], 1, .little);
    std.mem.writeInt(u64, bytes[24..32], 0, .little);
    bytes[32] = 'k';
    bytes[33] = 'v';
    var payload_checksum: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[0..34], &payload_checksum, .{});
    @memcpy(bytes[34..66], &payload_checksum);
    var chunk_hash: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(&bytes, &chunk_hash, .{});
    const logical_digest = chunk_hash;
    const descriptor = try (Descriptor{ .sequence = 7, .total = bytes.len, .direct_vectors = false, .logical_digest = logical_digest, .payload_checksum = payload_checksum, .chunk_hashes = &.{chunk_hash}, .effect_offsets = &.{16} }).encodeAlloc(alloc);
    defer alloc.free(descriptor);
    const Holder = struct {
        bytes: []u8,
        pub fn read(ptr: *anyopaque, _: u64, ordinal: u32, out: []u8) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (ordinal != 0 or out.len != self.bytes.len) return error.RetainedEffectsCorrupt;
            @memcpy(out, self.bytes);
            return out.len;
        }
    };
    var holder: Holder = .{ .bytes = &bytes };
    const scratch = try alloc.alloc(u8, chunk_bytes);
    defer alloc.free(scratch);
    var cache: View.ChunkCache = .{ .bytes = scratch };
    const view = try View.init(descriptor, 7, .{ .context = &holder, .read_chunk = Holder.read }, &cache);
    const effect = try view.effectAt(0, &cache);
    try std.testing.expectEqual(@as(u32, 34), try view.offsetAt(view.effect_count));
    try std.testing.expectEqual(@as(u32, 1), effect.key_len);
    try std.testing.expectEqual(@as(?u32, 1), effect.value_len);
    var value: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try view.readAt(effect.value_offset, &value, &cache));
    try std.testing.expectEqual(@as(u8, 'v'), value[0]);
    holder.bytes[33] = 'x';
    cache.invalidate();
    try std.testing.expectError(error.RetainedEffectsCorrupt, view.readAt(effect.value_offset, &value, &cache));
}
