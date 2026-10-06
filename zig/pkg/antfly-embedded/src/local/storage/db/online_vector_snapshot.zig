// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! One bounded vector from a certified portable object. The receiver persists
//! the object/offset position; index-grouped portable objects are not globally
//! ordered by document key and must never be resumed by a live key scan.
const std = @import("std");
const pages = @import("merge_page_contract.zig");
const codec = @import("enrichment/artifact_codec.zig");
const keys = @import("../internal_keys.zig");

/// A raw portable artifact value remains in its immutable object. Only its
/// bounded key/descriptor are resident while hashing and emitting chunks.
pub const RawDescriptor = struct {
    key: ?[]u8,
    object: u32,
    value_offset: u64,
    value_len: u32,
    next_position: pages.SnapshotPosition,

    pub fn read(self: RawDescriptor, reader: anytype, offset: u64, out: []u8) !void {
        if (offset > self.value_len or out.len > self.value_len - offset) return error.SourceSnapshotCorrupt;
        try exact(reader, self.object, try std.math.add(u64, self.value_offset, offset), out);
    }
};

/// Private graph objects preserve ownership/contender bytes, unlike public
/// portable graph exports. Their descriptor must never re-encode the value.
pub fn graphDescriptor(alloc: std.mem.Allocator, reader: anytype, object_size: u64, position: pages.SnapshotPosition, effect_protocol: u16) !?RawDescriptor {
    if (effect_protocol != 15) return error.OnlineMergeArtifactTailsUnsupported;
    const item = (try @import("source_artifact_batch.zig").descriptor(alloc, reader, object_size, .{
        .object = position.object,
        .offset = position.offset,
        .remaining = position.remaining,
    })) orelse return null;
    return .{
        .key = item.key,
        .object = item.object,
        .value_offset = item.value_offset,
        .value_len = item.value_len,
        .next_position = .{ .object = item.next_position.object, .offset = item.next_position.offset, .remaining = item.next_position.remaining },
    };
}

pub fn rawDescriptor(alloc: std.mem.Allocator, reader: anytype, object_size: u64, position: pages.SnapshotPosition) !?RawDescriptor {
    var next_position = position;
    if (next_position.offset == 0) {
        next_position.remaining = try number(u32, reader, position.object, 0);
        next_position.offset = 4;
    } else if (next_position.offset < 4) return error.SourceSnapshotCorrupt;
    if (next_position.remaining == 0) {
        if (next_position.offset != object_size) return error.SourceSnapshotCorrupt;
        return null;
    }
    const key_len = try number(u32, reader, position.object, next_position.offset);
    if (key_len == 0 or key_len > pages.max_cursor_bytes) return error.SourceSnapshotCorrupt;
    const key_offset = try std.math.add(u64, next_position.offset, 4);
    const length_offset = try std.math.add(u64, key_offset, key_len);
    const value_len = try number(u32, reader, position.object, length_offset);
    const value_offset = try std.math.add(u64, length_offset, 4);
    const end = try std.math.add(u64, value_offset, value_len);
    if (end > object_size) return error.SourceSnapshotCorrupt;
    const public_key = try alloc.alloc(u8, key_len);
    defer alloc.free(public_key);
    try exact(reader, position.object, key_offset, public_key);
    const ids = @import("artifact_ids.zig");
    var ref = (try ids.decodeArtifactPublicIdAlloc(alloc, public_key)) orelse return error.SourceSnapshotCorrupt;
    defer ref.deinit(alloc);
    const key: ?[]u8 = if (ref.kind == .embedding) vector: {
        if (ref.source != null or ref.chunk_id != null or ref.unit_id != null or value_len < codec.header_len + 4) return error.SourceSnapshotCorrupt;
        const internal = try ids.internalKeyForArtifactRefAlloc(alloc, ref);
        errdefer alloc.free(internal);
        if (!keys.isEmbeddingArtifactKey(internal) or internal.len > pages.max_cursor_bytes) return error.SourceSnapshotCorrupt;
        break :vector internal;
    } else if (ref.kind == .asset) null else return error.SourceSnapshotCorrupt;
    next_position.offset = end;
    next_position.remaining -= 1;
    return .{ .key = key, .object = position.object, .value_offset = value_offset, .value_len = value_len, .next_position = next_position };
}

fn exact(reader: anytype, object: u32, offset: u64, bytes: []u8) !void {
    var done: usize = 0;
    while (done < bytes.len) {
        const n = try reader.readAt(object, try std.math.add(u64, offset, done), bytes[done..]);
        if (n == 0) return error.SourceSnapshotCorrupt;
        done += n;
    }
}
fn number(comptime T: type, reader: anytype, object: u32, offset: u64) !T {
    var bytes: [@sizeOf(T)]u8 = undefined;
    try exact(reader, object, offset, &bytes);
    return std.mem.readInt(T, &bytes, .little);
}

pub fn next(alloc: std.mem.Allocator, reader: anytype, sparse: bool, object_size: u64, position: *pages.SnapshotPosition) !?pages.IntegrityEffect {
    const name_len = try number(u32, reader, position.object, 0);
    if (name_len == 0 or name_len > pages.max_cursor_bytes) return error.SourceSnapshotCorrupt;
    const name = try alloc.alloc(u8, name_len);
    defer alloc.free(name);
    try exact(reader, position.object, 4, name);
    const count_offset: u64 = @as(u64, name_len) + if (sparse) @as(u64, 4) else 6;
    const dimensions: u32 = if (sparse) 0 else try number(u16, reader, position.object, @as(u64, name_len) + 4);
    if (position.offset == 0) {
        position.remaining = try number(u32, reader, position.object, count_offset);
        position.offset = count_offset + 4;
    } else if (position.offset < count_offset + 4) return error.SourceSnapshotCorrupt;
    if (position.remaining == 0) {
        if (position.offset != object_size) return error.SourceSnapshotCorrupt;
        return null;
    }
    const doc_len = try number(u32, reader, position.object, position.offset);
    if (doc_len == 0 or doc_len > pages.max_cursor_bytes) return error.SourceSnapshotCorrupt;
    const doc_offset = try std.math.add(u64, position.offset, 4);
    const hash_offset = try std.math.add(u64, doc_offset, doc_len);
    const hash = try number(u64, reader, position.object, hash_offset);
    const count = if (sparse) try number(u32, reader, position.object, hash_offset + 8) else dimensions;
    const payload_offset = try std.math.add(u64, hash_offset, if (sparse) 12 else 8);
    const payload_len = try std.math.mul(usize, count, if (sparse) @as(usize, 8) else 4);
    if (payload_len > @import("../retained_effects.zig").max_frame_bytes) return error.SourceSnapshotCorrupt;
    const end = try std.math.add(u64, payload_offset, payload_len);
    if (end > object_size) return error.SourceSnapshotCorrupt;
    const doc = try alloc.alloc(u8, doc_len);
    defer alloc.free(doc);
    try exact(reader, position.object, doc_offset, doc);
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc, name);
    errdefer alloc.free(key);
    const raw = try alloc.alloc(u8, payload_len);
    defer alloc.free(raw);
    try exact(reader, position.object, payload_offset, raw);
    const values = try alloc.alloc(f32, count);
    defer alloc.free(values);
    const values_offset = if (sparse) @as(usize, count) * 4 else 0;
    for (values, 0..) |*value, i| value.* = @bitCast(std.mem.readInt(u32, raw[values_offset + 4 * i ..][0..4], .little));
    const value = if (sparse) sparse_value: {
        const indices = try alloc.alloc(u32, count);
        defer alloc.free(indices);
        for (indices, 0..) |*index, i| index.* = std.mem.readInt(u32, raw[4 * i ..][0..4], .little);
        break :sparse_value try codec.encodeSparseEmbeddingAlloc(alloc, hash, indices, values);
    } else try codec.encodeDenseEmbeddingAlloc(alloc, hash, values);
    errdefer alloc.free(value);
    @import("online_vector_artifacts.zig").validate(key, value) catch return error.SourceSnapshotCorrupt;
    position.offset = end;
    position.remaining -= 1;
    return .{ .key = key, .value = value };
}

test "online direct vector snapshot resumes dense and sparse certified object positions" {
    const alloc = std.testing.allocator;
    const backup = @import("../backup_codec.zig");
    const MemoryReader = struct {
        data: []const u8,
        fn readAt(self: *@This(), object: u32, offset: u64, out: []u8) !usize {
            if (object != 3 or offset > self.data.len) return error.SourceSnapshotCorrupt;
            // Short reads deliberately exercise the exact-read loop.
            const count = @min(@as(usize, 7), @min(out.len, self.data.len - @as(usize, @intCast(offset))));
            @memcpy(out[0..count], self.data[@intCast(offset)..][0..count]);
            return count;
        }
    };
    for ([_]bool{ false, true }) |sparse| {
        const raw = if (sparse)
            try backup.encodeSparseBatch(alloc, "vector", &.{ .{ .doc_key = "a", .hash_id = 1, .indices = &.{ 1, 3 }, .values = &.{ 2, 4 } }, .{ .doc_key = "z", .hash_id = 2, .indices = &.{5}, .values = &.{6} } })
        else
            try backup.encodeEmbeddingBatch(alloc, "vector", 2, &.{ .{ .doc_key = "a", .hash_id = 1, .vector = &.{ 2, 4 } }, .{ .doc_key = "z", .hash_id = 2, .vector = &.{ 6, 8 } } });
        defer alloc.free(raw);
        var reader: MemoryReader = .{ .data = raw };
        var position: pages.SnapshotPosition = .{ .object = 3, .offset = 0, .remaining = 0 };
        const first = (try next(alloc, &reader, sparse, raw.len, &position)).?;
        defer alloc.free(first.key);
        defer alloc.free(first.value.?);
        try std.testing.expectEqual(@as(u32, 1), position.remaining);
        // Recreate the reader with only the persisted position, as on failover.
        var restarted: MemoryReader = .{ .data = raw };
        const second = (try next(alloc, &restarted, sparse, raw.len, &position)).?;
        defer alloc.free(second.key);
        defer alloc.free(second.value.?);
        try std.testing.expect(std.mem.order(u8, first.key, second.key) == .lt);
        try std.testing.expectEqual(@as(u64, raw.len), position.offset);
        try std.testing.expect((try next(alloc, &restarted, sparse, raw.len, &position)) == null);
        var truncated: pages.SnapshotPosition = .{ .object = 3, .offset = 0, .remaining = 0 };
        try std.testing.expectError(error.SourceSnapshotCorrupt, next(alloc, &restarted, sparse, 1, &truncated));
    }
}

test "online graph snapshot descriptors require protocol fifteen and preserve certified source bytes" {
    const alloc = std.testing.allocator;
    const key = try keys.graphEdgeArtifactKeyAlloc(alloc, "owner", "g", "links", "target");
    defer alloc.free(key);
    const metadata = try alloc.alloc(u8, pages.chunk_bytes + 31);
    defer alloc.free(metadata);
    @memset(metadata, 'x');
    const value = try codec.encodeGraphEdgeAlloc(alloc, 19, 7, 0.5, 10, 11, metadata);
    defer alloc.free(value);
    const raw = try @import("../backup_codec.zig").encodeKeyValueBatch(alloc, &.{.{ .key = key, .value = value }});
    defer alloc.free(raw);
    _ = try @import("source_artifact_batch.zig").Reader.init(raw);
    const Memory = struct {
        bytes: []const u8,
        reads: usize = 0,
        pub fn readAt(self: *@This(), object: u32, offset: u64, out: []u8) !usize {
            self.reads += 1;
            if (object != 2 or offset > self.bytes.len) return error.SourceSnapshotCorrupt;
            const count = @min(@as(usize, 4093), @min(out.len, self.bytes.len - @as(usize, @intCast(offset))));
            @memcpy(out[0..count], self.bytes[@intCast(offset)..][0..count]);
            return count;
        }
    };
    var reader: Memory = .{ .bytes = raw };
    const start: pages.SnapshotPosition = .{ .object = 2, .offset = 0, .remaining = 0 };
    try std.testing.expectError(error.OnlineMergeArtifactTailsUnsupported, graphDescriptor(alloc, &reader, raw.len, start, 14));
    try std.testing.expectEqual(@as(usize, 0), reader.reads);
    var key_memory: [4096]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&key_memory);
    const item = (try graphDescriptor(bounded.allocator(), &reader, raw.len, start, 15)).?;
    try std.testing.expectEqualSlices(u8, key, item.key.?);
    try std.testing.expectEqual(value.len, item.value_len);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var scratch: [8192]u8 = undefined;
    var offset: usize = 0;
    while (offset < item.value_len) {
        const count = @min(scratch.len, item.value_len - offset);
        try item.read(&reader, offset, scratch[0..count]);
        try std.testing.expectEqualSlices(u8, value[offset..][0..count], scratch[0..count]);
        hash.update(scratch[0..count]);
        offset += count;
    }
    var expected: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &expected, .{});
    try std.testing.expectEqualSlices(u8, &expected, &hash.finalResult());
    var reopened: Memory = .{ .bytes = raw };
    try std.testing.expect((try graphDescriptor(alloc, &reopened, raw.len, item.next_position, 15)) == null);
    try std.testing.expectError(error.SourceSnapshotCorrupt, item.read(&reopened, item.value_len - 1, scratch[0..2]));
}
