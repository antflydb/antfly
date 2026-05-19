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

//! RaBitQ Vector Index Section - zapx-compatible segment format.
//!
//! Replaces FAISS with RaBitQ quantization for the vector index section.
//! Binary layout matches zapx's section framework so Go code can read
//! segments written by Zig and vice versa.
//!
//! On-disk format per field:
//!   [docvalue marker 1]     uvarint = 0xFFFFFFFFFFFFFFFF (fieldNotUninverted)
//!   [docvalue marker 2]     uvarint = 0xFFFFFFFFFFFFFFFF
//!   [optimization type]     uvarint (0=recall, 1=latency, 2=memory)
//!   [num vectors]           uvarint
//!   [vec→doc map size]      uvarint (0, reserved)
//!   [vecID→docID mapping]   numVecs × uvarint
//!   [index type]            uvarint (0=faiss, 1=rabitq)
//!   [index data size]       uvarint
//!   [index data]            bytes (serialized RaBitQ index)
//!
//! Index data (RaBitQ) layout:
//!   [magic]                 4 bytes = "RBIQ"
//!   [version]               u8 = 1
//!   [dims]                  u32 little-endian
//!   [metric]                u8 (0=l2, 1=inner_product, 2=cosine)
//!   [seed]                  u64 little-endian
//!   [num vectors]           u32 little-endian
//!   [raw vectors]           numVecs * dims * 4 bytes (float32, for reconstruction)
//!   [quantized set]         protobuf-encoded RaBitQuantizedVectorSet

const std = @import("std");
const Allocator = std.mem.Allocator;
const vec = @import("antfly_vector").vector;
const quantizer_mod = @import("antfly_vector").quantizer;
const proto = @import("antfly_vector").proto;

/// Sentinel value for "field not uninverted" (no doc values).
/// Matches zapx's fieldNotUninverted = math.MaxUint64.
const field_not_uninverted: u64 = 0xFFFFFFFFFFFFFFFF;

/// Index type identifiers (written as uvarint in section header).
pub const IndexType = enum(u8) {
    faiss = 0,
    rabitq = 1,
};

/// Optimization target for the vector index.
pub const OptimizationType = enum(u8) {
    recall_optimized = 0,
    latency_optimized = 1,
    memory_optimized = 2,
};

/// Magic bytes identifying a RaBitQ index blob.
const rabitq_magic = [4]u8{ 'R', 'B', 'I', 'Q' };
const rabitq_version: u8 = 1;

// ============================================================================
// Uvarint helpers (Go binary.PutUvarint / binary.Uvarint compatible)
// ============================================================================

fn writeUvarint(buf: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: u64) void {
    var v = value;
    while (v >= 0x80) {
        buf.append(alloc, @as(u8, @truncate(v)) | 0x80) catch unreachable;
        v >>= 7;
    }
    buf.append(alloc, @truncate(v)) catch unreachable;
}

fn readUvarint(data: []const u8, pos: *usize) u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (pos.* < data.len) {
        const b = data[pos.*];
        pos.* += 1;
        result |= @as(u64, b & 0x7F) << shift;
        if (b & 0x80 == 0) break;
        shift +|= 7;
    }
    return result;
}

// ============================================================================
// Vector Index Content (accumulates vectors during indexing)
// ============================================================================

/// Accumulates vectors for a single field during segment building.
pub const VectorIndexContent = struct {
    alloc: Allocator,
    dims: usize,
    metric: vec.DistanceMetric,
    optimization: OptimizationType,
    vectors: std.ArrayListUnmanaged(f32),
    vec_doc_ids: std.ArrayListUnmanaged(u32),

    pub fn init(alloc: Allocator, dims: usize, metric: vec.DistanceMetric, optimization: OptimizationType) VectorIndexContent {
        return .{
            .alloc = alloc,
            .dims = dims,
            .metric = metric,
            .optimization = optimization,
            .vectors = .empty,
            .vec_doc_ids = .empty,
        };
    }

    pub fn deinit(self: *VectorIndexContent) void {
        self.vectors.deinit(self.alloc);
        self.vec_doc_ids.deinit(self.alloc);
        self.* = undefined;
    }

    /// Add a vector for a document.
    pub fn addVector(self: *VectorIndexContent, vector: []const f32, doc_id: u32) !void {
        std.debug.assert(vector.len == self.dims);
        try self.vectors.appendSlice(self.alloc, vector);
        try self.vec_doc_ids.append(self.alloc, doc_id);
    }

    /// Number of vectors accumulated.
    pub fn count(self: *const VectorIndexContent) usize {
        return self.vec_doc_ids.items.len;
    }
};

// ============================================================================
// Section Writer
// ============================================================================

/// Writes a RaBitQ vector index section in zapx-compatible format.
pub fn writeVectorSection(
    alloc: Allocator,
    content: *const VectorIndexContent,
    seed: u64,
) ![]u8 {
    const nvecs = content.count();

    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(alloc);

    // --- Section header (zapx-compatible) ---

    // Doc value markers (fieldNotUninverted × 2)
    writeUvarint(&buf, alloc, field_not_uninverted);
    writeUvarint(&buf, alloc, field_not_uninverted);

    // Optimization type
    writeUvarint(&buf, alloc, @intFromEnum(content.optimization));

    // Number of vectors
    writeUvarint(&buf, alloc, @intCast(nvecs));

    // Vec→doc map size (reserved, always 0)
    writeUvarint(&buf, alloc, 0);

    // VecID → docID mapping (implicit sequential vecIDs)
    for (content.vec_doc_ids.items) |doc_id| {
        writeUvarint(&buf, alloc, @intCast(doc_id));
    }

    // Index type: rabitq = 1
    writeUvarint(&buf, alloc, @intFromEnum(IndexType.rabitq));

    // --- Build RaBitQ index data ---
    const index_data = try buildRaBitQIndex(alloc, content, seed);
    defer alloc.free(index_data);

    // Index data size
    writeUvarint(&buf, alloc, @intCast(index_data.len));

    // Index data
    try buf.appendSlice(alloc, index_data);

    return buf.toOwnedSlice(alloc);
}

/// Builds the serialized RaBitQ index blob.
fn buildRaBitQIndex(
    alloc: Allocator,
    content: *const VectorIndexContent,
    seed: u64,
) ![]u8 {
    const nvecs = content.count();
    const dims = content.dims;
    const vectors = content.vectors.items;

    const quant_vectors = if (content.metric == .cosine) blk: {
        const normalized = try alloc.dupe(f32, vectors);
        errdefer alloc.free(normalized);
        for (0..nvecs) |i| {
            _ = vec.normalize(normalized[i * dims ..][0..dims]);
        }
        break :blk normalized;
    } else vectors;
    defer if (content.metric == .cosine) alloc.free(quant_vectors);

    // Compute centroid (mean of all vectors)
    const centroid = try alloc.alloc(f32, dims);
    defer alloc.free(centroid);
    @memset(centroid, 0.0);

    for (0..nvecs) |i| {
        const v = quant_vectors[i * dims ..][0..dims];
        for (0..dims) |d| {
            centroid[d] += v[d];
        }
    }
    if (nvecs > 0) {
        const scale = 1.0 / @as(f32, @floatFromInt(nvecs));
        vec.scale(scale, centroid);
        if (content.metric == .cosine) _ = vec.normalize(centroid);
    }

    // Quantize all vectors
    var quantizer = try quantizer_mod.RaBitQuantizer.init(alloc, dims, seed, content.metric);
    defer quantizer.deinit();

    var qs = try quantizer.quantize(centroid, quant_vectors, nvecs);
    defer qs.deinit(alloc);

    // Serialize the quantized set
    const qs_bytes = try qs.encode(alloc);
    defer alloc.free(qs_bytes);

    // Build index blob
    var blob: std.ArrayListUnmanaged(u8) = .empty;
    errdefer blob.deinit(alloc);

    // Magic + version
    try blob.appendSlice(alloc, &rabitq_magic);
    try blob.append(alloc, rabitq_version);

    // Dims (u32 LE)
    const dims_le: [4]u8 = @bitCast(std.mem.nativeToLittle(u32, @intCast(dims)));
    try blob.appendSlice(alloc, &dims_le);

    // Metric (u8)
    try blob.append(alloc, @as(u8, @intCast(@intFromEnum(content.metric))));

    // Seed (u64 LE)
    const seed_le: [8]u8 = @bitCast(std.mem.nativeToLittle(u64, seed));
    try blob.appendSlice(alloc, &seed_le);

    // Num vectors (u32 LE)
    const nvecs_le: [4]u8 = @bitCast(std.mem.nativeToLittle(u32, @intCast(nvecs)));
    try blob.appendSlice(alloc, &nvecs_le);

    // Raw vectors (for reconstruction during merges)
    const raw_bytes: []const u8 = @as([*]const u8, @ptrCast(vectors.ptr))[0 .. nvecs * dims * 4];
    try blob.appendSlice(alloc, raw_bytes);

    // Quantized set (protobuf)
    const qs_size_le: [4]u8 = @bitCast(std.mem.nativeToLittle(u32, @intCast(qs_bytes.len)));
    try blob.appendSlice(alloc, &qs_size_le);
    try blob.appendSlice(alloc, qs_bytes);

    return blob.toOwnedSlice(alloc);
}

// ============================================================================
// Section Reader
// ============================================================================

/// Parsed vector section header.
pub const VectorSectionHeader = struct {
    optimization: OptimizationType,
    num_vecs: usize,
    vec_doc_ids: []u32,
    index_type: IndexType,
    index_data: []const u8,
};

/// Parsed RaBitQ index from the index data blob.
pub const RaBitQIndex = struct {
    dims: usize,
    metric: vec.DistanceMetric,
    seed: u64,
    num_vecs: usize,
    /// Raw vectors (float32, row-major). Owned, aligned copy.
    raw_vectors: []f32,
    /// Decoded quantized vector set. Owned.
    quantized_set: proto.RaBitQuantizedVectorSet,
    /// The quantizer (needed for search).
    quantizer: quantizer_mod.RaBitQuantizer,

    alloc: Allocator,
    vec_doc_ids_buf: []u32, // owned by the header parse

    pub fn deinit(self: *RaBitQIndex) void {
        self.alloc.free(self.raw_vectors);
        self.alloc.free(self.vec_doc_ids_buf);
        self.quantized_set.deinit(self.alloc);
        self.quantizer.deinit();
        self.* = undefined;
    }

    /// Search: estimate distances from query to all vectors.
    /// Returns top-k results as (doc_id, distance) pairs sorted by distance.
    pub fn search(
        self: *RaBitQIndex,
        query: []const f32,
        k: usize,
        vec_doc_ids: []const u32,
    ) !SearchResults {
        std.debug.assert(query.len == self.dims);

        const nvecs = self.num_vecs;
        const distances = try self.alloc.alloc(f32, nvecs);
        defer self.alloc.free(distances);
        const error_bounds = try self.alloc.alloc(f32, nvecs);
        defer self.alloc.free(error_bounds);

        const effective_query = if (self.metric == .cosine) blk: {
            const normalized = try self.alloc.dupe(f32, query);
            errdefer self.alloc.free(normalized);
            _ = vec.normalize(normalized);
            break :blk normalized;
        } else query;
        defer if (self.metric == .cosine) self.alloc.free(effective_query);

        try self.quantizer.estimateDistances(
            &self.quantized_set,
            effective_query,
            distances,
            error_bounds,
        );

        // Build results with doc IDs, keeping best score per doc
        const actual_k = @min(k, nvecs);
        var results = SearchResults{
            .hits = try self.alloc.alloc(SearchHit, actual_k),
            .len = 0,
            .alloc = self.alloc,
        };
        errdefer results.deinit();

        // Use a max-heap of size k to find top-k smallest distances
        // For simplicity, collect all and sort (fine for segment-level counts)
        const scored = try self.alloc.alloc(SearchHit, nvecs);
        defer self.alloc.free(scored);

        for (0..nvecs) |i| {
            scored[i] = .{
                .doc_id = vec_doc_ids[i],
                .score = distances[i],
                .error_bound = error_bounds[i],
            };
        }

        // Sort by distance (ascending for L2, descending for IP)
        if (self.metric == .inner_product) {
            std.mem.sort(SearchHit, scored, {}, SearchHit.lessThanIP);
        } else {
            std.mem.sort(SearchHit, scored, {}, SearchHit.lessThanL2);
        }

        // Deduplicate by doc_id, keeping best score
        var seen = std.AutoHashMap(u32, void).init(self.alloc);
        defer seen.deinit();

        var out_idx: usize = 0;
        for (scored) |hit| {
            if (out_idx >= actual_k) break;
            const gop = try seen.getOrPut(hit.doc_id);
            if (!gop.found_existing) {
                results.hits[out_idx] = hit;
                out_idx += 1;
            }
        }
        results.len = out_idx;

        return results;
    }
};

pub const SearchHit = struct {
    doc_id: u32,
    score: f32,
    error_bound: f32,

    fn lessThanL2(_: void, a: SearchHit, b: SearchHit) bool {
        return a.score < b.score;
    }

    fn lessThanIP(_: void, a: SearchHit, b: SearchHit) bool {
        // Inner product: higher is better, but we negate in estimateDistances
        return a.score < b.score;
    }
};

pub const SearchResults = struct {
    hits: []SearchHit,
    len: usize,
    alloc: Allocator,

    pub fn deinit(self: *SearchResults) void {
        self.alloc.free(self.hits);
        self.* = undefined;
    }

    pub fn getHits(self: *const SearchResults) []const SearchHit {
        return self.hits[0..self.len];
    }
};

/// Parse the section header from raw bytes.
/// `alloc` is used for the vec_doc_ids allocation.
pub fn readSectionHeader(alloc: Allocator, data: []const u8) !VectorSectionHeader {
    var pos: usize = 0;

    // Skip doc value markers
    _ = readUvarint(data, &pos); // marker 1
    _ = readUvarint(data, &pos); // marker 2

    // Optimization type
    const opt = readUvarint(data, &pos);

    // Number of vectors
    const nvecs = readUvarint(data, &pos);

    // Vec→doc map size (skip)
    _ = readUvarint(data, &pos);

    // VecID → docID mapping
    const vec_doc_ids = try alloc.alloc(u32, @intCast(nvecs));
    errdefer alloc.free(vec_doc_ids);
    for (0..@as(usize, @intCast(nvecs))) |i| {
        vec_doc_ids[i] = @intCast(readUvarint(data, &pos));
    }

    // Index type
    const index_type = readUvarint(data, &pos);

    // Index data
    const index_size = readUvarint(data, &pos);
    const index_data = data[pos..][0..@intCast(index_size)];

    return .{
        .optimization = @enumFromInt(@as(u8, @intCast(opt))),
        .num_vecs = @intCast(nvecs),
        .vec_doc_ids = vec_doc_ids,
        .index_type = @enumFromInt(@as(u8, @intCast(index_type))),
        .index_data = index_data,
    };
}

/// Parse a RaBitQ index from the index data blob.
pub fn readRaBitQIndex(alloc: Allocator, data: []const u8, vec_doc_ids: []u32) !RaBitQIndex {
    var pos: usize = 0;

    // Magic
    if (!std.mem.eql(u8, data[pos..][0..4], &rabitq_magic)) {
        return error.InvalidMagic;
    }
    pos += 4;

    // Version
    const version = data[pos];
    if (version != rabitq_version) {
        return error.UnsupportedVersion;
    }
    pos += 1;

    // Dims
    const dims: usize = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;

    // Metric
    const metric: vec.DistanceMetric = @enumFromInt(data[pos]);
    pos += 1;

    // Seed
    const seed = std.mem.readInt(u64, data[pos..][0..8], .little);
    pos += 8;

    // Num vectors
    const nvecs: usize = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;

    // Raw vectors (copy into aligned allocation)
    const raw_floats = nvecs * dims;
    const raw_bytes_len = raw_floats * 4;
    const raw_vectors = try alloc.alloc(f32, raw_floats);
    errdefer alloc.free(raw_vectors);
    @memcpy(std.mem.sliceAsBytes(raw_vectors), data[pos..][0..raw_bytes_len]);
    pos += raw_bytes_len;

    // Quantized set size + data
    const qs_size: usize = std.mem.readInt(u32, data[pos..][0..4], .little);
    pos += 4;
    const qs_bytes = data[pos..][0..qs_size];

    // Decode quantized set
    var qs = try proto.RaBitQuantizedVectorSet.decode(alloc, qs_bytes);
    errdefer qs.deinit(alloc);

    // Create quantizer
    var quantizer = try quantizer_mod.RaBitQuantizer.init(alloc, dims, seed, metric);
    errdefer quantizer.deinit();

    return .{
        .dims = dims,
        .metric = metric,
        .seed = seed,
        .num_vecs = nvecs,
        .raw_vectors = raw_vectors,
        .quantized_set = qs,
        .quantizer = quantizer,
        .alloc = alloc,
        .vec_doc_ids_buf = vec_doc_ids,
    };
}

/// Reconstruct original vectors by IDs (needed for segment merges).
/// Returns a newly allocated float32 slice with the reconstructed vectors.
pub fn reconstructVectors(
    alloc: Allocator,
    index: *const RaBitQIndex,
    vec_ids: []const u32,
) ![]f32 {
    const dims = index.dims;
    const result = try alloc.alloc(f32, vec_ids.len * dims);
    errdefer alloc.free(result);

    for (vec_ids, 0..) |vid, i| {
        const src = index.raw_vectors[vid * dims ..][0..dims];
        @memcpy(result[i * dims ..][0..dims], src);
    }

    return result;
}

// ============================================================================
// Tests
// ============================================================================

test "vector section write and read roundtrip" {
    const alloc = std.testing.allocator;

    // Create some test vectors
    const dims = 64;
    var content = VectorIndexContent.init(alloc, dims, .l2_squared, .latency_optimized);
    defer content.deinit();

    // Add 5 vectors
    var rng = std.Random.DefaultPrng.init(42);
    const random = rng.random();

    for (0..5) |doc_id| {
        var vector_buf: [dims]f32 = undefined;
        for (&vector_buf) |*v| {
            v.* = random.float(f32) * 2.0 - 1.0;
        }
        try content.addVector(&vector_buf, @intCast(doc_id * 10)); // doc IDs: 0, 10, 20, 30, 40
    }

    // Write section
    const section_bytes = try writeVectorSection(alloc, &content, 42);
    defer alloc.free(section_bytes);

    // Read header
    const header = try readSectionHeader(alloc, section_bytes);

    try std.testing.expectEqual(@as(usize, 5), header.num_vecs);
    try std.testing.expectEqual(OptimizationType.latency_optimized, header.optimization);
    try std.testing.expectEqual(IndexType.rabitq, header.index_type);

    // Check doc IDs
    try std.testing.expectEqual(@as(u32, 0), header.vec_doc_ids[0]);
    try std.testing.expectEqual(@as(u32, 10), header.vec_doc_ids[1]);
    try std.testing.expectEqual(@as(u32, 40), header.vec_doc_ids[4]);

    // Read RaBitQ index (takes ownership of vec_doc_ids)
    var index = try readRaBitQIndex(alloc, header.index_data, header.vec_doc_ids);
    defer index.deinit();

    try std.testing.expectEqual(@as(usize, 64), index.dims);
    try std.testing.expectEqual(vec.DistanceMetric.l2_squared, index.metric);
    try std.testing.expectEqual(@as(usize, 5), index.num_vecs);
}

test "vector section search returns nearest" {
    const alloc = std.testing.allocator;
    const dims = 64;

    var content = VectorIndexContent.init(alloc, dims, .l2_squared, .latency_optimized);
    defer content.deinit();

    // Vector 0: all positive
    var v0: [dims]f32 = undefined;
    const mag: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(dims)));
    @memset(&v0, mag);
    try content.addVector(&v0, 100);

    // Vector 1: all negative
    var v1: [dims]f32 = undefined;
    @memset(&v1, -mag);
    try content.addVector(&v1, 200);

    // Write and read
    const section_bytes = try writeVectorSection(alloc, &content, 42);
    defer alloc.free(section_bytes);

    const header = try readSectionHeader(alloc, section_bytes);
    var index = try readRaBitQIndex(alloc, header.index_data, header.vec_doc_ids);
    defer index.deinit();

    // Search with query similar to vector 0
    var results = try index.search(&v0, 2, index.vec_doc_ids_buf);
    defer results.deinit();

    const hits = results.getHits();
    try std.testing.expect(hits.len >= 1);
    // Nearest to v0 should be doc 100
    try std.testing.expectEqual(@as(u32, 100), hits[0].doc_id);
}

test "vector section reconstruct vectors" {
    const alloc = std.testing.allocator;
    const dims = 8;

    var content = VectorIndexContent.init(alloc, dims, .l2_squared, .recall_optimized);
    defer content.deinit();

    const v0 = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const v1 = [_]f32{ 8, 7, 6, 5, 4, 3, 2, 1 };
    try content.addVector(&v0, 0);
    try content.addVector(&v1, 1);

    const section_bytes = try writeVectorSection(alloc, &content, 42);
    defer alloc.free(section_bytes);

    const header = try readSectionHeader(alloc, section_bytes);
    var index = try readRaBitQIndex(alloc, header.index_data, header.vec_doc_ids);
    defer index.deinit();

    // Reconstruct vector 1
    const reconstructed = try reconstructVectors(alloc, &index, &[_]u32{1});
    defer alloc.free(reconstructed);

    try std.testing.expectEqual(@as(usize, dims), reconstructed.len);
    for (0..dims) |d| {
        try std.testing.expectApproxEqAbs(v1[d], reconstructed[d], 1e-6);
    }
}
