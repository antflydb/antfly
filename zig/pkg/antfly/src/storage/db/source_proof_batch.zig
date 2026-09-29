// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Authenticated source-copy provenance. These are inert donor proof bodies
//! plus selected-output bitmaps, never copied receipt or authority records.
const std = @import("std");
const provenance = @import("artifact_producer_provenance.zig");
const publication = @import("artifact_publication.zig");

pub const import_prefix = "\x00\x00__metadata__:source_proof:";
pub const merge_prefix = "\x00\x00__metadata__:merge_source_proof:";
pub const max_entries: usize = 65536;
pub const max_bytes: usize = @import("../backup_codec.zig").max_block_payload_bytes;
const record_magic = "SPR1";

pub fn importKey(namespace: publication.Namespace, digest: publication.Digest) [import_prefix.len + 24 + 32]u8 {
    var result: [import_prefix.len + 24 + 32]u8 = undefined;
    @memcpy(result[0..import_prefix.len], import_prefix);
    @memcpy(result[import_prefix.len..][0..24], &namespace);
    @memcpy(result[import_prefix.len + 24 ..], &digest);
    return result;
}

/// Merge evidence is cut-scoped so a later attempt cannot overwrite the
/// selected-output bitmap from a different certified source pin.
pub fn mergeKey(namespace: publication.Namespace, pin: [32]u8, digest: publication.Digest) [merge_prefix.len + 24 + 32 + 32]u8 {
    var result: [merge_prefix.len + 24 + 32 + 32]u8 = undefined;
    @memcpy(result[0..merge_prefix.len], merge_prefix);
    @memcpy(result[merge_prefix.len..][0..24], &namespace);
    @memcpy(result[merge_prefix.len + 24 ..][0..32], &pin);
    @memcpy(result[merge_prefix.len + 56 ..], &digest);
    return result;
}

/// Only inert evidence keys use this namespace. Donor receipt/authority keys
/// are never legal merge-page effects.
pub fn transferDigest(namespace: publication.Namespace, pin: [32]u8, key: []const u8) !publication.Digest {
    if (key.len != merge_prefix.len + 24 + 32 + 32 or !std.mem.startsWith(u8, key, merge_prefix) or
        !std.mem.eql(u8, key[merge_prefix.len..][0..24], &namespace) or
        !std.mem.eql(u8, key[merge_prefix.len + 24 ..][0..32], &pin)) return error.InvalidMergePage;
    return key[merge_prefix.len + 56 ..][0..32].*;
}

pub fn encodeValueAlloc(alloc: std.mem.Allocator, bitmap: []const u8, proof: []const u8) ![]u8 {
    if (bitmap.len == 0 or bitmap.len > publication.max_source_documents / 8 or proof.len > provenance.max_encoded_bytes or
        bitmap.len +| proof.len +| 6 > max_bytes - 44) return error.ResourceLimitExceeded;
    const value = try alloc.alloc(u8, 6 + bitmap.len + proof.len);
    @memcpy(value[0..4], record_magic);
    std.mem.writeInt(u16, value[4..6], @intCast(bitmap.len), .little);
    @memcpy(value[6..][0..bitmap.len], bitmap);
    @memcpy(value[6 + bitmap.len ..], proof);
    return value;
}

pub const Decoded = struct {
    /// Proof slices and bitmap borrow the certified batch value.
    proof: provenance.Owned,
    bitmap: []const u8,
    pub fn deinit(self: *@This()) void {
        self.proof.deinit();
        self.* = undefined;
    }
};

pub fn decodeValue(alloc: std.mem.Allocator, namespace: publication.Namespace, digest: publication.Digest, raw: []const u8) !Decoded {
    if (raw.len < 6 + 40 or raw.len > max_bytes - 44 or !std.mem.eql(u8, raw[0..4], record_magic)) return error.SourceSnapshotCorrupt;
    const bitmap_len = std.mem.readInt(u16, raw[4..6], .little);
    if (bitmap_len == 0 or bitmap_len > publication.max_source_documents / 8 or raw.len < 6 + @as(usize, bitmap_len) + 40)
        return error.SourceSnapshotCorrupt;
    const bitmap = raw[6..][0..bitmap_len];
    var proof = provenance.decodeBorrowed(alloc, raw[6 + bitmap_len ..]) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.SourceSnapshotCorrupt,
    };
    errdefer proof.deinit();
    if (!std.mem.eql(u8, &proof.proof.namespace, &namespace) or !std.mem.eql(u8, &proof.proof.publication_digest, &digest) or
        bitmap.len != (proof.proof.sources.len + 7) / 8) return error.SourceSnapshotCorrupt;
    var outputs = std.StaticBitSet(publication.max_source_documents).initEmpty();
    for (proof.proof.effects) |effect| outputs.set(effect.source_index);
    var selected = false;
    for (bitmap, 0..) |bits, byte_index| {
        for (0..8) |bit_index| {
            if (bits & (@as(u8, 1) << @intCast(bit_index)) == 0) continue;
            const ordinal = byte_index * 8 + bit_index;
            if (ordinal >= proof.proof.sources.len or !outputs.isSet(ordinal)) return error.SourceSnapshotCorrupt;
            selected = true;
        }
    }
    if (!selected) return error.SourceSnapshotCorrupt;
    return .{ .proof = proof, .bitmap = bitmap };
}

pub const Entry = struct { digest: publication.Digest, value: []const u8 };
pub const Position = struct { object: u32, offset: u64 = 0, remaining: u32 = 0 };
pub const Descriptor = struct {
    digest: publication.Digest,
    object: u32,
    value_offset: u64,
    value_len: u32,
    next_position: Position,

    pub fn read(self: Descriptor, reader: anytype, offset: u64, out: []u8) !void {
        if (offset > self.value_len or out.len > self.value_len - offset) return error.SourceSnapshotCorrupt;
        try exact(reader, self.object, self.value_offset + offset, out);
    }
};

fn exact(reader: anytype, object: u32, offset: u64, out: []u8) !void {
    var done: usize = 0;
    while (done < out.len) {
        const count = try reader.readAt(object, offset + done, out[done..]);
        if (count == 0 or count > out.len - done) return error.SourceSnapshotCorrupt;
        done += count;
    }
}

fn readWord(reader: anytype, object: u32, offset: u64) !u32 {
    var bytes: [4]u8 = undefined;
    try exact(reader, object, offset, &bytes);
    return std.mem.readInt(u32, &bytes, .little);
}

/// Locate one record without copying its potentially large APF2 body. A
/// source-certificate verifier authenticates the complete object before the
/// merge driver uses these offsets; the receiver validates the record again
/// before granting any local adoption evidence.
pub fn descriptor(reader: anytype, object_size: u64, position: Position) !?Descriptor {
    if (object_size < 4 or object_size > max_bytes) return error.SourceSnapshotCorrupt;
    var next = position;
    if (next.offset == 0) {
        if (next.remaining != 0) return error.SourceSnapshotCorrupt;
        next.remaining = try readWord(reader, next.object, 0);
        if ((next.remaining == 0 and object_size != 4) or next.remaining > max_entries or next.remaining > (object_size - 4) / 86)
            return error.SourceSnapshotCorrupt;
        next.offset = 4;
    }
    if (next.offset < 4 or next.offset > object_size or next.remaining > max_entries) return error.SourceSnapshotCorrupt;
    if (next.remaining == 0) {
        if (next.offset != object_size) return error.SourceSnapshotCorrupt;
        return null;
    }
    if (object_size - next.offset < 40) return error.SourceSnapshotCorrupt;
    if (try readWord(reader, next.object, next.offset) != 32) return error.SourceSnapshotCorrupt;
    var digest: publication.Digest = undefined;
    try exact(reader, next.object, next.offset + 4, &digest);
    const value_len = try readWord(reader, next.object, next.offset + 36);
    if (value_len < 46 or value_len > max_bytes - 44 or value_len > object_size - next.offset - 40)
        return error.SourceSnapshotCorrupt;
    const value_offset = next.offset + 40;
    const end = value_offset + value_len;
    next.offset = end;
    next.remaining -= 1;
    if ((next.remaining == 0 and end != object_size) or next.remaining > (object_size - end) / 86)
        return error.SourceSnapshotCorrupt;
    return .{ .digest = digest, .object = position.object, .value_offset = value_offset, .value_len = value_len, .next_position = next };
}

pub const Reader = struct {
    bytes: []const u8,
    offset: usize = 4,
    remaining: u32,

    pub fn init(bytes: []const u8) !Reader {
        if (bytes.len < 4 or bytes.len > max_bytes) return error.SourceSnapshotCorrupt;
        const count = std.mem.readInt(u32, bytes[0..4], .little);
        if ((count == 0 and bytes.len != 4) or count > max_entries or count > (bytes.len - 4) / 8) return error.SourceSnapshotCorrupt;
        return .{ .bytes = bytes, .remaining = count };
    }

    fn take(self: *Reader, length: usize) ![]const u8 {
        if (length > self.bytes.len -| self.offset) return error.SourceSnapshotCorrupt;
        const result = self.bytes[self.offset..][0..length];
        self.offset += length;
        return result;
    }

    pub fn next(self: *Reader) !?Entry {
        if (self.remaining == 0) {
            if (self.offset != self.bytes.len) return error.SourceSnapshotCorrupt;
            return null;
        }
        const key_len = std.mem.readInt(u32, (try self.take(4))[0..4], .little);
        if (key_len != 32) return error.SourceSnapshotCorrupt;
        const digest: publication.Digest = (try self.take(32))[0..32].*;
        const value_len = std.mem.readInt(u32, (try self.take(4))[0..4], .little);
        if (value_len < 46 or value_len > max_bytes - 44) return error.SourceSnapshotCorrupt;
        const value = try self.take(value_len);
        self.remaining -= 1;
        return .{ .digest = digest, .value = value };
    }
};

test "ordered artifact inventory source proof batch rejects forged bitmap and count without granting authority" {
    const alloc = std.testing.allocator;
    const source = publication.Source{ .document_key = "doc", .content_digest = @splat(3), .timestamp = 1, .input_position = null };
    const effect = provenance.Effect{ .family = .document_artifact, .key = "effect", .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: provenance.Proof = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    const bytes = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(bytes);
    const value = try encodeValueAlloc(alloc, &.{1}, bytes);
    defer alloc.free(value);
    var decoded = try decodeValue(alloc, proof.namespace, proof.publication_digest, value);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &.{1}, decoded.bitmap);
    const forged = try alloc.dupe(u8, value);
    defer alloc.free(forged);
    forged[6] = 2;
    try std.testing.expectError(error.SourceSnapshotCorrupt, decodeValue(alloc, proof.namespace, proof.publication_digest, forged));
    try std.testing.expectError(error.SourceSnapshotCorrupt, Reader.init("\xff\xff\xff\x7f"));
}

test "ordered artifact inventory source proof descriptor resumes a certified large body" {
    const alloc = std.testing.allocator;
    const document = try alloc.alloc(u8, 2 * 1024 * 1024);
    defer alloc.free(document);
    @memset(document, 'd');
    const source = publication.Source{ .document_key = document, .content_digest = @splat(3), .timestamp = 1, .input_position = null };
    const effect = provenance.Effect{ .family = .document_artifact, .key = "effect", .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: provenance.Proof = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    const raw = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(raw);
    const value = try encodeValueAlloc(alloc, &.{1}, raw);
    defer alloc.free(value);
    const batch = try @import("../backup_codec.zig").encodeKeyValueBatch(alloc, &.{.{ .key = &proof.publication_digest, .value = value }});
    defer alloc.free(batch);
    const Mock = struct {
        bytes: []const u8,
        fn readAt(self: *@This(), object: u32, offset: u64, out: []u8) !usize {
            if (object != 7 or offset > self.bytes.len) return 0;
            const count = @min(out.len, self.bytes.len - @as(usize, @intCast(offset)));
            if (count == 0) return 0;
            @memcpy(out[0..count], self.bytes[@intCast(offset)..][0..count]);
            return count;
        }
    };
    var reader: Mock = .{ .bytes = batch };
    const first = (try descriptor(&reader, batch.len, .{ .object = 7 })).?;
    try std.testing.expectEqualSlices(u8, &proof.publication_digest, &first.digest);
    try std.testing.expectEqual(@as(u32, @intCast(value.len)), first.value_len);
    var sample: [17]u8 = undefined;
    const crossing_offset: usize = 1024 * 1024 - 8;
    try first.read(&reader, crossing_offset, &sample);
    try std.testing.expectEqualSlices(u8, value[crossing_offset..][0..sample.len], &sample);
    try std.testing.expect((try descriptor(&reader, batch.len, first.next_position)) == null);
    try std.testing.expectError(error.SourceSnapshotCorrupt, first.read(&reader, value.len - 1, sample[0..2]));
    try std.testing.expectError(error.SourceSnapshotCorrupt, descriptor(&reader, batch.len - 1, .{ .object = 7 }));
    try std.testing.expectError(error.SourceSnapshotCorrupt, descriptor(&reader, batch.len, .{ .object = 7, .offset = 0, .remaining = 1 }));
}

test "ordered artifact inventory merge proof payload is isolated and chunk-resumable" {
    const alloc = std.testing.allocator;
    const pages = @import("merge_page_contract.zig");
    const types = @import("types.zig");
    const identity: @import("doc_identity_namespace.zig").Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    var namespace: publication.Namespace = undefined;
    @import("doc_identity.zig").encodeNamespace(&namespace, identity);
    const document = try alloc.alloc(u8, 2 * pages.chunk_bytes);
    defer alloc.free(document);
    @memset(document, 'd');
    const source = publication.Source{ .document_key = document, .content_digest = @splat(3), .timestamp = 1, .input_position = null };
    const effect = provenance.Effect{ .family = .document_artifact, .key = "effect", .source_index = 0, .value_digest = null, .value_bytes = 0 };
    var proof: provenance.Proof = .{ .namespace = namespace, .authority_epoch = 1, .catalog_digest = @splat(2), .producer_kind = .index, .producer_name = "index", .producer_generation = 1, .producer_artifact_name = "asset", .publication_digest = @splat(4), .input_digest = undefined, .sources = (&source)[0..1], .artifact_sources = &.{}, .effects = (&effect)[0..1] };
    proof.input_digest = proof.inputCommand().inputDigest();
    const raw = try provenance.encodeAlloc(alloc, proof);
    defer alloc.free(raw);
    const value = try encodeValueAlloc(alloc, &.{1}, raw);
    defer alloc.free(value);
    const import_key = mergeKey(namespace, @splat(1), proof.publication_digest);
    var request: types.BatchRequest = .{
        .merge_replication = .{ .transition_id = 7, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } },
        .merge_page = .{ .source = .{ .namespace = identity, .pin_digest = @splat(1), .applied_index = 1, .retention = .{ .epoch = 1, .after_sequence = 0 }, .provenance_required = true }, .sequence = 1, .phase = .artifacts, .next = &import_key, .exhausted = false, .digest = @splat(0), .next_snapshot_position = .{ .object = 7, .offset = 64, .remaining = 0 }, .provenance_effects = &.{.{ .key = &import_key, .value = value }} },
    };
    request.merge_page.?.digest = pages.commandDigest(request);
    try pages.validateRequest(request);
    const chunks = try pages.RowChunks(types.BatchRequest).init(request);
    const first = try chunks.requestAt(0);
    const second = try chunks.requestAt(pages.chunk_bytes);
    const final = try chunks.requestAt(2 * pages.chunk_bytes);
    try std.testing.expectEqual(pages.ChunkPayload.provenance, first.merge_page.?.chunk.?.payload);
    try std.testing.expect(!first.merge_page.?.chunk.?.complete());
    try std.testing.expect(!second.merge_page.?.chunk.?.complete());
    try std.testing.expect(final.merge_page.?.chunk.?.complete());
    const encoded = try std.json.Stringify.valueAlloc(alloc, first.merge_page.?, .{});
    defer alloc.free(encoded);
    var decoded = try std.json.parseFromSlice(pages.Command, alloc, encoded, .{});
    defer decoded.deinit();
    try std.testing.expectEqual(pages.ChunkPayload.provenance, decoded.value.chunk.?.payload);
    try std.testing.expectEqualSlices(u8, first.merge_page.?.chunk.?.data, decoded.value.chunk.?.data);
    const wrong_key = mergeKey(@splat(9), @splat(1), proof.publication_digest);
    request.merge_page.?.provenance_effects = &.{.{ .key = &wrong_key, .value = value }};
    request.merge_page.?.next = &wrong_key;
    request.merge_page.?.digest = pages.commandDigest(request);
    try std.testing.expectError(error.InvalidMergePage, pages.validateRequest(request));
}
