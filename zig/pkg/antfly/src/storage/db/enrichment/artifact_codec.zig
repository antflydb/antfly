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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const native_endian = builtin.target.cpu.arch.endian();
const edge_weight = @import("../../../graph/edge_weight.zig");

pub const codec_version: u16 = 1;
pub const graph_edge_codec_version: u16 = 2;
pub const magic: [8]u8 = .{ 'A', 'F', 'E', 'N', 'R', 'C', 'H', 0 };
pub const header_len: usize = magic.len + @sizeOf(u16) + @sizeOf(u8) + @sizeOf(u8) + @sizeOf(u64) + @sizeOf(u32);

pub const Kind = enum(u8) {
    chunk_json = 1,
    dense_embedding = 2,
    sparse_embedding = 3,
    asset = 4,
    graph_edge = 5,
};

pub const Flags = packed struct(u8) {
    has_source_hash: bool = false,
    has_graph_generation: bool = false,
    portable_unbound_graph_generation: bool = false,
    _reserved: u5 = 0,
};

pub const Header = struct {
    version: u16,
    kind: Kind,
    flags: Flags,
    source_hash: u64,
    payload_len: u32,
};

pub fn hashSource(source: []const u8) u64 {
    return std.hash.XxHash64.hash(0, source);
}

pub fn hashEmbeddingSource(source: []const u8, semantic_producer: []const u8) u64 {
    if (semantic_producer.len == 0) return hashSource(source);
    var hasher = std.hash.XxHash64.init(0);
    var length: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &length, @intCast(source.len), .little);
    hasher.update(&length);
    hasher.update(source);
    std.mem.writeInt(u64, &length, @intCast(semantic_producer.len), .little);
    hasher.update(&length);
    hasher.update(semantic_producer);
    return hasher.final();
}

pub fn encodeDenseEmbeddingAlloc(alloc: Allocator, source_hash: ?u64, vector: []const f32) ![]u8 {
    const payload_len = @sizeOf(u32) + vector.len * @sizeOf(u32);
    const total_len = header_len + payload_len;
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    writeHeader(out[0..header_len], .{
        .version = codec_version,
        .kind = .dense_embedding,
        .flags = .{ .has_source_hash = source_hash != null },
        .source_hash = source_hash orelse 0,
        .payload_len = @intCast(payload_len),
    });

    var pos: usize = header_len;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(vector.len), .little);
    pos += @sizeOf(u32);
    for (vector) |value| {
        std.mem.writeInt(u32, out[pos..][0..4], @as(u32, @bitCast(value)), .little);
        pos += @sizeOf(u32);
    }
    return out;
}

pub fn decodeDenseEmbeddingAlloc(alloc: Allocator, data: []const u8) ![]f32 {
    const dims = try decodeDenseEmbeddingDims(data);
    const vector = try alloc.alloc(f32, dims);
    errdefer alloc.free(vector);
    _ = try decodeDenseEmbeddingInto(data, vector);
    return vector;
}

pub fn denseEmbeddingVectorView(data: []const u8) !?[]const f32 {
    if (native_endian != .little) return null;
    const payload = try denseEmbeddingPayload(data);
    const dims = std.mem.readInt(u32, payload[0..4], .little);
    const vector_bytes = payload[@sizeOf(u32)..];
    if ((@intFromPtr(vector_bytes.ptr) % @alignOf(f32)) != 0) return null;
    const aligned: []align(@alignOf(f32)) const u8 = @alignCast(vector_bytes);
    return std.mem.bytesAsSlice(f32, aligned)[0..dims];
}

pub fn decodeDenseEmbeddingViewOrInto(data: []const u8, out: []f32) ![]const f32 {
    if (try denseEmbeddingVectorView(data)) |view| return view;
    return try decodeDenseEmbeddingInto(data, out);
}

pub fn decodeDenseEmbeddingInto(data: []const u8, out: []f32) ![]const f32 {
    const payload = try denseEmbeddingPayload(data);
    const dims = std.mem.readInt(u32, payload[0..4], .little);
    if (dims > out.len) return error.BufferTooSmall;

    var pos: usize = @sizeOf(u32);
    for (out[0..dims]) |*value| {
        value.* = @bitCast(std.mem.readInt(u32, payload[pos..][0..4], .little));
        pos += @sizeOf(u32);
    }
    return out[0..dims];
}

pub fn decodeDenseEmbeddingDims(data: []const u8) !u32 {
    const payload = try denseEmbeddingPayload(data);
    return std.mem.readInt(u32, payload[0..4], .little);
}

fn denseEmbeddingPayload(data: []const u8) ![]const u8 {
    const header = try decodeHeader(data);
    if (header.kind != .dense_embedding) return error.InvalidArtifactKind;
    if (header.payload_len < @sizeOf(u32)) return error.InvalidArtifactPayload;

    const payload = data[header_len..][0..header.payload_len];
    const dims = std.mem.readInt(u32, payload[0..4], .little);
    const vector_bytes = payload.len - @sizeOf(u32);
    if (vector_bytes != @as(usize, dims) * @sizeOf(u32)) return error.InvalidVectorDimensions;
    return payload;
}

pub fn decodeDenseEmbeddingJsonVectorAlloc(alloc: Allocator, data: []const u8) !std.json.Value {
    const vector = try decodeDenseEmbeddingAlloc(alloc, data);
    defer alloc.free(vector);

    var arr = std.json.Array.init(alloc);
    errdefer arr.deinit();
    for (vector) |value| {
        try arr.append(.{ .float = @floatCast(value) });
    }
    return .{ .array = arr };
}

pub const SparseEmbedding = struct {
    indices: []u32,
    values: []f32,

    pub fn deinit(self: *SparseEmbedding, alloc: Allocator) void {
        alloc.free(self.indices);
        alloc.free(self.values);
        self.* = undefined;
    }
};

pub const SparseEmbeddingView = struct {
    indices: []const u32,
    values: []const f32,
};

pub const GraphEdge = struct {
    generation: u64,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []u8,

    pub fn deinit(self: *GraphEdge, alloc: Allocator) void {
        alloc.free(self.metadata_json);
        self.* = undefined;
    }
};

pub fn encodeSparseEmbeddingAlloc(alloc: Allocator, source_hash: ?u64, indices: []const u32, values: []const f32) ![]u8 {
    if (indices.len != values.len) return error.InvalidSparseEmbedding;
    const payload_len = @sizeOf(u32) + indices.len * (@sizeOf(u32) + @sizeOf(u32));
    const total_len = header_len + payload_len;
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    writeHeader(out[0..header_len], .{
        .version = codec_version,
        .kind = .sparse_embedding,
        .flags = .{ .has_source_hash = source_hash != null },
        .source_hash = source_hash orelse 0,
        .payload_len = @intCast(payload_len),
    });

    var pos: usize = header_len;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(indices.len), .little);
    pos += @sizeOf(u32);
    for (indices) |index| {
        std.mem.writeInt(u32, out[pos..][0..4], index, .little);
        pos += @sizeOf(u32);
    }
    for (values) |value| {
        std.mem.writeInt(u32, out[pos..][0..4], @as(u32, @bitCast(value)), .little);
        pos += @sizeOf(u32);
    }
    return out;
}

pub fn decodeSparseEmbeddingAlloc(alloc: Allocator, data: []const u8) !SparseEmbedding {
    const payload = try sparseEmbeddingPayload(data);
    const count = std.mem.readInt(u32, payload[0..4], .little);

    const indices = try alloc.alloc(u32, count);
    errdefer alloc.free(indices);
    const values = try alloc.alloc(f32, count);
    errdefer alloc.free(values);

    var pos: usize = @sizeOf(u32);
    for (indices) |*index| {
        index.* = std.mem.readInt(u32, payload[pos..][0..4], .little);
        pos += @sizeOf(u32);
    }
    for (values) |*value| {
        value.* = @bitCast(std.mem.readInt(u32, payload[pos..][0..4], .little));
        pos += @sizeOf(u32);
    }
    return .{ .indices = indices, .values = values };
}

pub fn sparseEmbeddingVectorView(data: []const u8) !?SparseEmbeddingView {
    if (native_endian != .little) return null;
    const payload = try sparseEmbeddingPayload(data);
    const count = std.mem.readInt(u32, payload[0..4], .little);
    if (count == 0) return .{ .indices = &.{}, .values = &.{} };

    const indices_start = @sizeOf(u32);
    const indices_end = indices_start + @as(usize, count) * @sizeOf(u32);
    const values_end = indices_end + @as(usize, count) * @sizeOf(u32);
    const indices_bytes = payload[indices_start..indices_end];
    const values_bytes = payload[indices_end..values_end];
    if ((@intFromPtr(indices_bytes.ptr) % @alignOf(u32)) != 0) return null;
    if ((@intFromPtr(values_bytes.ptr) % @alignOf(f32)) != 0) return null;

    const aligned_indices: []align(@alignOf(u32)) const u8 = @alignCast(indices_bytes);
    const aligned_values: []align(@alignOf(f32)) const u8 = @alignCast(values_bytes);
    return .{
        .indices = std.mem.bytesAsSlice(u32, aligned_indices),
        .values = std.mem.bytesAsSlice(f32, aligned_values),
    };
}

fn sparseEmbeddingPayload(data: []const u8) ![]const u8 {
    const header = try decodeHeader(data);
    if (header.kind != .sparse_embedding) return error.InvalidArtifactKind;
    if (header.payload_len < @sizeOf(u32)) return error.InvalidArtifactPayload;

    const payload = data[header_len..][0..header.payload_len];
    const count = std.mem.readInt(u32, payload[0..4], .little);
    const pair_bytes = payload.len - @sizeOf(u32);
    if (pair_bytes != @as(usize, count) * (@sizeOf(u32) + @sizeOf(u32))) return error.InvalidSparseEmbedding;
    return payload;
}

pub fn encodeGraphEdgeAlloc(
    alloc: Allocator,
    source_hash: ?u64,
    generation: u64,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,
) ![]u8 {
    try edge_weight.validateStored(weight);
    const payload_len = @sizeOf(u64) * 4 + @sizeOf(u32) + metadata_json.len;
    const total_len = header_len + payload_len;
    const out = try alloc.alloc(u8, total_len);
    errdefer alloc.free(out);

    writeHeader(out[0..header_len], .{
        .version = graph_edge_codec_version,
        .kind = .graph_edge,
        .flags = .{ .has_source_hash = source_hash != null, .has_graph_generation = true },
        .source_hash = source_hash orelse 0,
        .payload_len = @intCast(payload_len),
    });

    var pos: usize = header_len;
    std.mem.writeInt(u64, out[pos..][0..8], generation, .little);
    pos += @sizeOf(u64);
    std.mem.writeInt(u64, out[pos..][0..8], @as(u64, @bitCast(weight)), .little);
    pos += @sizeOf(u64);
    std.mem.writeInt(u64, out[pos..][0..8], created_at, .little);
    pos += @sizeOf(u64);
    std.mem.writeInt(u64, out[pos..][0..8], updated_at, .little);
    pos += @sizeOf(u64);
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(metadata_json.len), .little);
    pos += @sizeOf(u32);
    @memcpy(out[pos .. pos + metadata_json.len], metadata_json);
    return out;
}

test "graph edge artifacts reject weights outside the durable domain" {
    try std.testing.expectError(
        error.InvalidGraphEdges,
        encodeGraphEdgeAlloc(std.testing.allocator, null, 1, -0.1, 0, 0, ""),
    );
    try std.testing.expectError(
        error.InvalidGraphEdges,
        encodeGraphEdgeAlloc(std.testing.allocator, null, 1, std.math.inf(f64), 0, 0, ""),
    );
}

pub fn encodePortableUnboundGraphEdgeAlloc(
    alloc: Allocator,
    weight: f64,
    created_at: u64,
    updated_at: u64,
    metadata_json: []const u8,
) ![]u8 {
    const out = try encodeGraphEdgeAlloc(alloc, null, 0, weight, created_at, updated_at, metadata_json);
    var header = try decodeHeader(out);
    header.flags.portable_unbound_graph_generation = true;
    writeHeader(out[0..header_len], header);
    return out;
}

pub fn isPortableUnboundGraphEdge(data: []const u8) bool {
    const header = decodeHeader(data) catch return false;
    if (header.kind != .graph_edge or !header.flags.portable_unbound_graph_generation) return false;
    if (header.payload_len < @sizeOf(u64) or data.len < header_len + header.payload_len) return false;
    return std.mem.readInt(u64, data[header_len..][0..8], .little) == 0;
}

/// Generation-less v1 records predate incarnation fencing. Callers must not
/// bind them unless a generation-bearing owner manifest authenticates the
/// payload. Explicitly portable records may be bound by an active producer.
/// Version 2 makes new records self-describing and ensures an older binary
/// rejects them instead of interpreting the generation as weight.
pub fn needsGraphGenerationBinding(data: []const u8) bool {
    return isLegacyUnboundGraphEdge(data) or isPortableUnboundGraphEdge(data);
}

pub fn isLegacyUnboundGraphEdge(data: []const u8) bool {
    const header = decodeHeader(data) catch return false;
    if (header.kind != .graph_edge) return false;
    return header.version == codec_version;
}

pub fn bindGraphEdgeGenerationAlloc(alloc: Allocator, data: []const u8, generation: u64) ![]u8 {
    if (!needsGraphGenerationBinding(data)) return error.GraphGenerationAlreadyBound;

    const header = try decodeHeader(data);
    var decoded = try decodeGraphEdgeAlloc(alloc, data);
    defer decoded.deinit(alloc);
    return encodeGraphEdgeAlloc(
        alloc,
        if (header.flags.has_source_hash) header.source_hash else null,
        generation,
        decoded.weight,
        decoded.created_at,
        decoded.updated_at,
        decoded.metadata_json,
    );
}

/// Returns a generation-authenticated copy of a graph edge, or null when the
/// payload is malformed or belongs to a different index incarnation. A
/// current generation-bearing state manifest is sufficient authority to bind
/// legacy and explicitly portable payloads exactly once.
pub fn authenticateGraphEdgeGenerationAlloc(alloc: Allocator, data: []const u8, generation: u64) !?[]u8 {
    if (needsGraphGenerationBinding(data)) {
        return bindGraphEdgeGenerationAlloc(alloc, data, generation) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return null,
        };
    }
    var decoded = decodeGraphEdgeAlloc(alloc, data) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return null,
    };
    defer decoded.deinit(alloc);
    if (decoded.generation != generation) return null;
    return try alloc.dupe(u8, data);
}

pub fn decodeGraphEdgeAlloc(alloc: Allocator, data: []const u8) !GraphEdge {
    const header = try decodeHeader(data);
    if (header.kind != .graph_edge) return error.InvalidArtifactKind;

    const payload = data[header_len..][0..header.payload_len];
    var pos: usize = 0;
    const generation: u64 = if (header.version == graph_edge_codec_version) blk: {
        if (!header.flags.has_graph_generation or header.payload_len < @sizeOf(u64) * 4 + @sizeOf(u32)) return error.InvalidArtifactPayload;
        const value = std.mem.readInt(u64, payload[pos..][0..8], .little);
        pos += @sizeOf(u64);
        break :blk value;
    } else blk: {
        if (header.flags.has_graph_generation or header.payload_len < @sizeOf(u64) * 3 + @sizeOf(u32)) return error.InvalidArtifactPayload;
        break :blk 0;
    };
    const weight = @as(f64, @bitCast(std.mem.readInt(u64, payload[pos..][0..8], .little)));
    if (!edge_weight.isStoredValid(weight)) return error.InvalidArtifactPayload;
    pos += @sizeOf(u64);
    const created_at = std.mem.readInt(u64, payload[pos..][0..8], .little);
    pos += @sizeOf(u64);
    const updated_at = std.mem.readInt(u64, payload[pos..][0..8], .little);
    pos += @sizeOf(u64);
    const metadata_len = std.mem.readInt(u32, payload[pos..][0..4], .little);
    pos += @sizeOf(u32);
    if (payload.len != pos + metadata_len) return error.InvalidArtifactPayload;

    return .{
        .generation = generation,
        .weight = weight,
        .created_at = created_at,
        .updated_at = updated_at,
        .metadata_json = try alloc.dupe(u8, payload[pos..]),
    };
}

pub fn decodeHeader(data: []const u8) !Header {
    if (data.len < header_len) return error.InvalidArtifactHeader;
    if (!std.mem.eql(u8, data[0..magic.len], &magic)) return error.InvalidArtifactMagic;

    var pos: usize = magic.len;
    const version = std.mem.readInt(u16, data[pos..][0..2], .little);
    pos += @sizeOf(u16);
    const kind_raw = data[pos];
    pos += @sizeOf(u8);
    const kind: Kind = switch (kind_raw) {
        @backingInt(Kind.chunk_json) => .chunk_json,
        @backingInt(Kind.dense_embedding) => .dense_embedding,
        @backingInt(Kind.sparse_embedding) => .sparse_embedding,
        @backingInt(Kind.asset) => .asset,
        @backingInt(Kind.graph_edge) => .graph_edge,
        else => return error.InvalidArtifactKind,
    };
    if (version != codec_version and !(version == graph_edge_codec_version and kind == .graph_edge)) {
        return error.UnsupportedArtifactCodecVersion;
    }

    const flags: Flags = @bitCast(data[pos]);
    pos += @sizeOf(u8);

    const source_hash = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += @sizeOf(u64);

    const payload_len = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += @sizeOf(u32);
    if (pos != header_len) return error.InvalidArtifactHeader;
    if (data.len != header_len + @as(usize, payload_len)) return error.InvalidArtifactPayload;

    return .{
        .version = version,
        .kind = kind,
        .flags = flags,
        .source_hash = source_hash,
        .payload_len = payload_len,
    };
}

pub fn sourceHash(data: []const u8) !?u64 {
    const header = try decodeHeader(data);
    if (!header.flags.has_source_hash) return null;
    return header.source_hash;
}

fn writeHeader(dst: []u8, header: Header) void {
    std.debug.assert(dst.len == header_len);
    @memcpy(dst[0..magic.len], &magic);
    var pos: usize = magic.len;
    std.mem.writeInt(u16, dst[pos..][0..2], header.version, .little);
    pos += @sizeOf(u16);
    dst[pos] = @backingInt(header.kind);
    pos += @sizeOf(u8);
    dst[pos] = @bitCast(header.flags);
    pos += @sizeOf(u8);
    std.mem.writeInt(u64, dst[pos..][0..8], header.source_hash, .little);
    pos += @sizeOf(u64);
    std.mem.writeInt(u32, dst[pos..][0..4], header.payload_len, .little);
}

test "artifact codec encodes dense embedding with version and source hash" {
    const alloc = std.testing.allocator;
    const hash = hashSource("hello world");
    const encoded = try encodeDenseEmbeddingAlloc(alloc, hash, &.{ 1.25, -2.5, 3.0 });
    defer alloc.free(encoded);

    const header = try decodeHeader(encoded);
    try std.testing.expectEqual(codec_version, header.version);
    try std.testing.expectEqual(Kind.dense_embedding, header.kind);
    try std.testing.expect(header.flags.has_source_hash);
    try std.testing.expectEqual(hash, header.source_hash);
    try std.testing.expectEqual(@as(?u64, hash), try sourceHash(encoded));

    const decoded = try decodeDenseEmbeddingAlloc(alloc, encoded);
    defer alloc.free(decoded);
    try std.testing.expectEqual(@as(usize, 3), decoded.len);
    try std.testing.expectEqual(@as(f32, 1.25), decoded[0]);
    try std.testing.expectEqual(@as(f32, -2.5), decoded[1]);
    try std.testing.expectEqual(@as(f32, 3.0), decoded[2]);

    if (try denseEmbeddingVectorView(encoded)) |view| {
        try std.testing.expectEqualSlices(f32, &.{ 1.25, -2.5, 3.0 }, view);
    } else {
        try std.testing.expect(native_endian != .little);
    }
}

test "embedding source hash binds semantic producer identity" {
    try std.testing.expectEqual(hashSource("same text"), hashEmbeddingSource("same text", ""));
    try std.testing.expectEqual(
        hashEmbeddingSource("same text", "{\"model\":\"embed-v1\"}"),
        hashEmbeddingSource("same text", "{\"model\":\"embed-v1\"}"),
    );
    try std.testing.expect(
        hashEmbeddingSource("same text", "{\"model\":\"embed-v1\"}") !=
            hashEmbeddingSource("same text", "{\"model\":\"embed-v2\"}"),
    );
    try std.testing.expect(hashEmbeddingSource("ab", "c") != hashEmbeddingSource("a", "bc"));
}

test "artifact codec falls back to scratch for unaligned dense embedding view" {
    const alloc = std.testing.allocator;
    const encoded = try encodeDenseEmbeddingAlloc(alloc, null, &.{ 4.0, 5.0, 6.0 });
    defer alloc.free(encoded);

    const unaligned_storage = try alloc.alloc(u8, encoded.len + 1);
    defer alloc.free(unaligned_storage);
    @memcpy(unaligned_storage[1..], encoded);
    const unaligned = unaligned_storage[1..];

    if (native_endian == .little and (@intFromPtr((unaligned[header_len + @sizeOf(u32) ..]).ptr) % @alignOf(f32)) != 0) {
        try std.testing.expectEqual(@as(?[]const f32, null), try denseEmbeddingVectorView(unaligned));
    }

    var scratch: [3]f32 = undefined;
    const decoded = try decodeDenseEmbeddingViewOrInto(unaligned, &scratch);
    try std.testing.expectEqualSlices(f32, &.{ 4.0, 5.0, 6.0 }, decoded);
    try std.testing.expectEqual(decoded.ptr, scratch[0..].ptr);
}

test "artifact codec rejects non-artifact bytes" {
    try std.testing.expectError(error.InvalidArtifactHeader, decodeHeader("short"));
    try std.testing.expectError(error.InvalidArtifactMagic, decodeHeader("notmagic-not-an-artifact"));
}

test "artifact codec encodes sparse embedding with version and source hash" {
    const alloc = std.testing.allocator;
    const hash = hashSource("sparse source");
    const encoded = try encodeSparseEmbeddingAlloc(alloc, hash, &.{ 3, 9 }, &.{ 0.25, 1.5 });
    defer alloc.free(encoded);

    const header = try decodeHeader(encoded);
    try std.testing.expectEqual(codec_version, header.version);
    try std.testing.expectEqual(Kind.sparse_embedding, header.kind);
    try std.testing.expect(header.flags.has_source_hash);
    try std.testing.expectEqual(hash, header.source_hash);
    try std.testing.expectEqual(@as(?u64, hash), try sourceHash(encoded));
    try std.testing.expectEqual(@as(u32, 2), std.mem.readInt(u32, encoded[header_len..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, encoded[header_len + 4 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, 9), std.mem.readInt(u32, encoded[header_len + 8 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, @bitCast(@as(f32, 0.25))), std.mem.readInt(u32, encoded[header_len + 12 ..][0..4], .little));
    try std.testing.expectEqual(@as(u32, @bitCast(@as(f32, 1.5))), std.mem.readInt(u32, encoded[header_len + 16 ..][0..4], .little));

    var decoded = try decodeSparseEmbeddingAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(u32, &.{ 3, 9 }, decoded.indices);
    try std.testing.expectEqual(@as(f32, 0.25), decoded.values[0]);
    try std.testing.expectEqual(@as(f32, 1.5), decoded.values[1]);

    if (try sparseEmbeddingVectorView(encoded)) |view| {
        try std.testing.expectEqualSlices(u32, &.{ 3, 9 }, view.indices);
        try std.testing.expectEqualSlices(f32, &.{ 0.25, 1.5 }, view.values);
    } else {
        try std.testing.expect(native_endian != .little);
    }
}

test "artifact codec sparse embedding view falls back when unaligned" {
    const alloc = std.testing.allocator;
    const encoded = try encodeSparseEmbeddingAlloc(alloc, null, &.{ 1, 2, 3 }, &.{ 0.5, 0.25, 0.125 });
    defer alloc.free(encoded);

    const unaligned_storage = try alloc.alloc(u8, encoded.len + 1);
    defer alloc.free(unaligned_storage);
    @memcpy(unaligned_storage[1..], encoded);
    const unaligned = unaligned_storage[1..];

    if (native_endian == .little and (@intFromPtr((unaligned[header_len + @sizeOf(u32) ..]).ptr) % @alignOf(u32)) != 0) {
        try std.testing.expectEqual(@as(?SparseEmbeddingView, null), try sparseEmbeddingVectorView(unaligned));
    }

    var decoded = try decodeSparseEmbeddingAlloc(alloc, unaligned);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualSlices(u32, &.{ 1, 2, 3 }, decoded.indices);
    try std.testing.expectEqualSlices(f32, &.{ 0.5, 0.25, 0.125 }, decoded.values);
}

test "artifact codec encodes graph edge with version and source hash" {
    const alloc = std.testing.allocator;
    const hash = hashSource("graph source");
    const encoded = try encodeGraphEdgeAlloc(alloc, hash, 42, 1.5, 10, 20, "{\"k\":1}");
    defer alloc.free(encoded);

    const header = try decodeHeader(encoded);
    try std.testing.expectEqual(graph_edge_codec_version, header.version);
    try std.testing.expectEqual(Kind.graph_edge, header.kind);
    try std.testing.expect(header.flags.has_source_hash);
    try std.testing.expectEqual(hash, header.source_hash);

    var decoded = try decodeGraphEdgeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 42), decoded.generation);
    try std.testing.expectEqual(@as(f64, 1.5), decoded.weight);
    try std.testing.expectEqual(@as(u64, 10), decoded.created_at);
    try std.testing.expectEqual(@as(u64, 20), decoded.updated_at);
    try std.testing.expectEqualStrings("{\"k\":1}", decoded.metadata_json);
}

test "artifact codec rejects weights outside the durable domain" {
    const alloc = std.testing.allocator;
    for ([_]f64{ -0.1, std.math.nan(f64), std.math.inf(f64), -std.math.inf(f64) }) |weight| {
        try std.testing.expectError(
            error.InvalidGraphEdges,
            encodeGraphEdgeAlloc(alloc, null, 42, weight, 0, 0, "{}"),
        );
    }
}

test "artifact codec binds portable graph edge generation exactly once" {
    const alloc = std.testing.allocator;
    const portable = try encodePortableUnboundGraphEdgeAlloc(alloc, 1.5, 10, 20, "{\"k\":1}");
    defer alloc.free(portable);

    const bound = try bindGraphEdgeGenerationAlloc(alloc, portable, 42);
    defer alloc.free(bound);
    try std.testing.expect(!isPortableUnboundGraphEdge(bound));

    var decoded = try decodeGraphEdgeAlloc(alloc, bound);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 42), decoded.generation);
    try std.testing.expectEqualStrings("{\"k\":1}", decoded.metadata_json);
    try std.testing.expectError(error.GraphGenerationAlreadyBound, bindGraphEdgeGenerationAlloc(alloc, bound, 43));
}

test "artifact codec generation binding preserves source identity" {
    const alloc = std.testing.allocator;
    const source_hash = hashSource("graph source");
    const portable = try encodePortableUnboundGraphEdgeAlloc(alloc, 1.5, 10, 20, "{}");
    defer alloc.free(portable);
    var header = try decodeHeader(portable);
    header.flags.has_source_hash = true;
    header.source_hash = source_hash;
    writeHeader(portable[0..header_len], header);

    const bound = try bindGraphEdgeGenerationAlloc(alloc, portable, 42);
    defer alloc.free(bound);
    const bound_header = try decodeHeader(bound);
    try std.testing.expect(bound_header.flags.has_source_hash);
    try std.testing.expectEqual(source_hash, bound_header.source_hash);
}

test "artifact codec authenticates graph edge incarnation" {
    const alloc = std.testing.allocator;
    const current = try encodeGraphEdgeAlloc(alloc, null, 42, 1, 0, 0, "{}");
    defer alloc.free(current);
    const stale = try encodeGraphEdgeAlloc(alloc, null, 41, 1, 0, 0, "{}");
    defer alloc.free(stale);

    const authenticated = (try authenticateGraphEdgeGenerationAlloc(alloc, current, 42)).?;
    defer alloc.free(authenticated);
    try std.testing.expectEqualSlices(u8, current, authenticated);
    try std.testing.expect((try authenticateGraphEdgeGenerationAlloc(alloc, stale, 42)) == null);
    try std.testing.expect((try authenticateGraphEdgeGenerationAlloc(alloc, "corrupt", 42)) == null);
}

test "artifact codec reads and fences generation-less v1 graph edges" {
    const alloc = std.testing.allocator;
    const legacy =
        "AFENRCH\x00" ++
        "\x01\x00\x05\x00" ++
        "\x00\x00\x00\x00\x00\x00\x00\x00" ++
        "\x23\x00\x00\x00" ++
        "\x00\x00\x00\x00\x00\x00\xf8\x3f" ++
        "\x0a\x00\x00\x00\x00\x00\x00\x00" ++
        "\x14\x00\x00\x00\x00\x00\x00\x00" ++
        "\x07\x00\x00\x00{\"k\":1}";

    var decoded = try decodeGraphEdgeAlloc(alloc, legacy);
    defer decoded.deinit(alloc);
    try std.testing.expect(needsGraphGenerationBinding(legacy));
    try std.testing.expectEqual(@as(u64, 0), decoded.generation);
    try std.testing.expectEqual(@as(f64, 1.5), decoded.weight);
    try std.testing.expectEqualStrings("{\"k\":1}", decoded.metadata_json);

    const bound = try bindGraphEdgeGenerationAlloc(alloc, legacy, 42);
    defer alloc.free(bound);
    try std.testing.expect(!needsGraphGenerationBinding(bound));
    var rebound = try decodeGraphEdgeAlloc(alloc, bound);
    defer rebound.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 42), rebound.generation);
}
