// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Object-backed compact candidate planes and lossless source residual packs.
//! The native vector-block codec owns quantization and bit-exact reconstruction.
//! Every deferred pack is SHA-256 authenticated before any source value escapes.
const std = @import("std");
const A = std.mem.Allocator;
const codec = @import("antfly_vectorindex").vector_block;
const vectors = @import("antfly_vector").vector;
const common = @import("../paged_projection.zig");
const PageStore = @import("../graph_segment/page_store.zig").PageStore;
const segment = @import("mod.zig");
const marker: u32 = 0x80000000;
const metadata_bytes = 88;
pub const max_pack_bytes = 128 * 1024;

pub fn isCompact(value: []const u8) bool {
    return value.len >= 8 and std.mem.readInt(u32, value[4..8], .little) & marker != 0;
}
pub const Control = struct {
    id: []const u8,
    projection: []const u8,
    scale: f32,
    error_norm: f32,
    norm_lower: f32,
    residual: common.Blob,
    offset: u32,
    length: u32,
    source_lsn: u64,
    source_ordinal: u32,
};
pub fn control(value: []const u8, dims: u32) !Control {
    if (!isCompact(value) or dims == 0 or dims > 16384) return error.InvalidVectorValue;
    const id_len = std.mem.readInt(u32, value[0..4], .little);
    if (std.mem.readInt(u32, value[4..8], .little) != (marker | dims) or
        @as(u64, id_len) + 8 + metadata_bytes + @as(u64, dims) * 2 != value.len) return error.InvalidVectorValue;
    const data = value[8..];
    const result = Control{
        .id = value[8 + metadata_bytes ..][0..id_len],
        .scale = @bitCast(std.mem.readInt(u32, data[0..4], .little)),
        .error_norm = @bitCast(std.mem.readInt(u32, data[4..8], .little)),
        .norm_lower = @bitCast(std.mem.readInt(u32, data[8..12], .little)),
        .residual = try common.Blob.decode(data[12..68]),
        .offset = std.mem.readInt(u32, data[68..72], .little),
        .length = std.mem.readInt(u32, data[72..76], .little),
        .source_lsn = std.mem.readInt(u64, data[76..84], .little),
        .source_ordinal = std.mem.readInt(u32, data[84..88], .little),
        .projection = data[metadata_bytes + @as(usize, id_len) ..],
    };
    if (result.id.len == 0 or !std.math.isFinite(result.scale) or result.scale <= 0 or
        !std.math.isFinite(result.error_norm) or result.error_norm < 0 or !std.math.isFinite(result.norm_lower) or result.norm_lower < 0 or
        result.residual.bytes > max_pack_bytes or result.length == 0 or result.length > result.residual.bytes or
        result.offset > result.residual.bytes - result.length) return error.InvalidVectorValue;
    return result;
}

/// Returns owned controls, with residuals uploaded before references are bound.
/// Batches pack at most sixteen records and 128 KiB; exceptional residuals flush
/// early. Failure can leave collectible scoped blobs but never publishes a root.
pub fn encodeAlloc(a: A, pages: *PageStore, entries: []const segment.Entry) ![][]u8 {
    const output = try a.alloc([]u8, entries.len);
    var initialized: usize = 0;
    errdefer {
        for (output[0..initialized]) |bytes| a.free(bytes);
        a.free(output);
    }
    var begin: usize = 0;
    while (begin < entries.len) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var pack: std.ArrayList(u8) = .empty;
        var end = begin;
        var offsets: [16]u32 = undefined;
        var lengths: [16]u32 = undefined;
        var controls: [16][]u8 = undefined;
        while (end < entries.len and end - begin < controls.len) {
            try pages.cancellation.check();
            const entry = entries[end];
            if (entry.doc_id.len == 0 or entry.vector.len == 0 or entry.vector.len > 16384) return error.InvalidVectorValue;
            const encoded = try scratch.alloc(u8, entry.vector.len * 2);
            const stats = try codec.encodeVectorIntoWithStats(.float16, entry.vector, encoded);
            if (!std.math.isFinite(stats.quantization.error_norm) or !std.math.isFinite(stats.quantization.decoded_norm_lower_bound)) return error.VectorResidualTooLarge;
            const residual = try scratch.alloc(u8, try codec.exactResidualMaxBytes(entry.vector.len));
            const len = try codec.encodeExactResidualInto(entry.vector, encoded, stats.scale, residual);
            // Extreme vectors can exceed the compact pack bound: retain the
            // legacy inline lossless representation instead of dropping bits.
            if (len > max_pack_bytes) return error.VectorResidualTooLarge;
            if (pack.items.len + len > max_pack_bytes and end != begin) break;
            offsets[end - begin] = @intCast(pack.items.len);
            lengths[end - begin] = @intCast(len);
            try pack.appendSlice(scratch, residual[0..len]);
            const value = try scratch.alloc(u8, 8 + entry.doc_id.len + metadata_bytes + encoded.len);
            std.mem.writeInt(u32, value[0..4], @intCast(entry.doc_id.len), .little);
            std.mem.writeInt(u32, value[4..8], marker | @as(u32, @intCast(entry.vector.len)), .little);
            @memcpy(value[8 + metadata_bytes ..][0..entry.doc_id.len], entry.doc_id);
            const data = value[8..];
            std.mem.writeInt(u32, data[0..4], @bitCast(stats.scale), .little);
            std.mem.writeInt(u32, data[4..8], @bitCast(stats.quantization.error_norm), .little);
            std.mem.writeInt(u32, data[8..12], @bitCast(stats.quantization.decoded_norm_lower_bound), .little);
            std.mem.writeInt(u64, data[76..84], entry.source_lsn, .little);
            std.mem.writeInt(u32, data[84..88], entry.source_ordinal, .little);
            @memcpy(data[metadata_bytes + entry.doc_id.len ..], encoded);
            controls[end - begin] = value;
            end += 1;
        }
        const blob = try upload(pages, pack.items);
        for (controls[0 .. end - begin], offsets[0 .. end - begin], lengths[0 .. end - begin]) |value, offset, len| {
            const data = value[8..];
            @memcpy(data[12..68], &blob.encode());
            std.mem.writeInt(u32, data[68..72], offset, .little);
            std.mem.writeInt(u32, data[72..76], len, .little);
            output[initialized] = try a.dupe(u8, value);
            initialized += 1;
        }
        begin = end;
    }
    return output;
}

fn upload(pages: *PageStore, bytes: []const u8) !common.Blob {
    if (bytes.len == 0 or bytes.len > max_pack_bytes or bytes.len > pages.remaining_write_bytes.*) return error.GraphPageWriteBudgetExceeded;
    pages.remaining_write_bytes.* -= bytes.len;
    const scope = @import("../artifacts/store.zig").UploadScope{ .domain = pages.domain, .attempt = pages.attempt };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const id = try scope.artifactId(&std.fmt.bytesToHex(digest, .lower));
    var metadata = try pages.artifacts.putScoped(scope, bytes, pages.cancellation);
    defer metadata.deinit(pages.artifacts.allocator);
    if (metadata.byte_len != bytes.len or !std.mem.eql(u8, metadata.artifact_id, &id) or !std.mem.eql(u8, metadata.checksum, id[7..71])) return error.ArtifactIntegrityMismatch;
    return .{ .digest = digest, .attempt = pages.attempt, .bytes = bytes.len };
}

pub const Reader = struct {
    a: A,
    pages: *PageStore,
    slots: std.ArrayList(?Slot) = .empty,
    max_bytes: usize = 512 * 1024,
    retained: usize = 0,
    fetches: usize = 0,
    clock: u64 = 0,
    read_count: ?*usize = null,
    const Slot = struct { source: common.Blob, domain: [32]u8, bytes: []u8, clock: u64 };
    pub fn deinit(self: *Reader) void {
        for (self.slots.items) |slot| if (slot) |s| self.a.free(s.bytes);
        self.slots.deinit(self.a);
    }
    fn load(self: *Reader, source: common.Blob) ![]const u8 {
        try self.pages.cancellation.check();
        self.clock +%= 1;
        var victim: ?usize = null;
        for (self.slots.items, 0..) |*slot, i| {
            if (slot.*) |*s| {
                if (std.meta.eql(s.source, source) and std.mem.eql(u8, &s.domain, &self.pages.domain)) {
                    s.clock = self.clock;
                    return s.bytes;
                }
            } else victim = i;
        }
        if (self.read_count) |count| {
            if (count.* >= 128) return error.ExactScanYield;
            count.* += 1;
        }
        self.fetches += 1;
        const id = try source.identity(self.pages.domain);
        const checksum = std.fmt.bytesToHex(source.digest, .lower);
        const bytes = if (self.pages.read_cache) |cache|
            try cache.read(cache.ptr, self.a, self.pages.artifacts, .{ .kind = .vector_segment, .artifact_id = &id, .checksum = &checksum, .byte_len = source.bytes }, 0, @intCast(source.bytes), source.digest, self.pages.cancellation, self.pages.remaining_read_bytes)
        else blk: {
            try @import("../artifacts/store.zig").chargeReadBudget(self.pages.remaining_read_bytes, source.bytes);
            break :blk try self.pages.artifacts.getRangeAllocWithCancellationUsingAllocator(self.a, &id, 0, @intCast(source.bytes), self.pages.cancellation);
        };
        errdefer self.a.free(bytes);
        // Validate on both cached and plain transports before admitting a pack.
        if (bytes.len != source.bytes) return error.ArtifactIntegrityMismatch;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (!std.mem.eql(u8, &digest, &source.digest)) return error.ArtifactIntegrityMismatch;
        try self.pages.cancellation.check();
        while (self.retained + bytes.len > self.max_bytes or (victim == null and self.slots.items.len == 4096)) {
            var oldest: ?usize = null;
            for (self.slots.items, 0..) |slot, i| if (slot) |s| {
                if (oldest == null or s.clock < self.slots.items[oldest.?].?.clock) oldest = i;
            };
            const index = oldest orelse return error.VectorResidualTooLarge;
            self.retained -= self.slots.items[index].?.bytes.len;
            self.a.free(self.slots.items[index].?.bytes);
            self.slots.items[index] = null;
            victim = index;
        }
        const entry = Slot{ .source = source, .domain = self.pages.domain, .bytes = bytes, .clock = self.clock };
        if (victim) |i| {
            std.debug.assert(self.slots.items[i] == null);
            self.slots.items[i] = entry;
        } else {
            if (self.slots.items.len == self.slots.capacity) try self.slots.ensureTotalCapacityPrecise(self.a, @min(4096, @max(8, self.slots.capacity * 2)));
            self.slots.appendAssumeCapacity(entry);
        }
        self.retained += bytes.len;
        return bytes;
    }
    pub fn read(self: *Reader, value: []const u8, dims: u32) !segment.Entry {
        try self.pages.cancellation.check();
        if (!isCompact(value)) return @import("paged.zig").decodeVector(self.a, value, dims);
        const c = try control(value, dims);
        const pack_bytes = try self.load(c.residual);
        const vector = try self.a.alloc(f32, dims);
        errdefer self.a.free(vector);
        _ = try codec.decodeExactResidualInto(c.projection, dims, c.scale, pack_bytes[c.offset..][0..c.length], vector);
        return .{ .doc_id = try self.a.dupe(u8, c.id), .vector = vector, .source_lsn = c.source_lsn, .source_ordinal = c.source_ordinal };
    }
};

/// Repack a bounded sorted merge slice without changing its candidate codes,
/// quantization certificates or source bits. Only residual locators change;
/// legacy inline rows remain lossless and readable until ordinary reindex.
pub fn repackAlloc(a: A, pages: *PageStore, values: []const []const u8, dims: u32) ![][]u8 {
    var reader = Reader{ .a = a, .pages = pages, .max_bytes = 4 * 1024 * 1024 };
    defer reader.deinit();
    const output = try a.alloc([]u8, values.len);
    var initialized: usize = 0;
    errdefer {
        for (output[0..initialized]) |value| a.free(value);
        a.free(output);
    }
    var verify: [16384]f32 = undefined;
    var begin: usize = 0;
    while (begin < values.len) {
        var pack: std.ArrayList(u8) = .empty;
        defer pack.deinit(a);
        var positions: [16]usize = undefined;
        var offsets: [16]u32 = undefined;
        var patches: usize = 0;
        var end = begin;
        while (end < values.len and end - begin < 16) {
            try pages.cancellation.check();
            const old = values[end];
            if (isCompact(old)) {
                const c = try control(old, dims);
                if (pack.items.len != 0 and c.length > max_pack_bytes - pack.items.len) break;
                const source = try reader.load(c.residual);
                const residual = source[c.offset..][0..c.length];
                _ = try codec.decodeExactResidualInto(c.projection, dims, c.scale, residual, &verify);
                offsets[patches] = @intCast(pack.items.len);
                positions[patches] = end;
                patches += 1;
                try pack.appendSlice(a, residual);
            } else if (old.len != 0) {
                var legacy = try @import("paged.zig").decodeVector(a, old, dims);
                legacy.deinit(a);
            }
            output[end] = try a.dupe(u8, old);
            initialized += 1;
            end += 1;
        }
        if (patches != 0) {
            const blob = try upload(pages, pack.items);
            for (positions[0..patches], offsets[0..patches]) |position, offset| {
                const value = output[position];
                const data = value[8..];
                @memcpy(data[12..68], &blob.encode());
                std.mem.writeInt(u32, data[68..72], offset, .little);
            }
        }
        begin = end;
    }
    return output;
}

/// Conservative lower distance bound; equality must always refine. Persisted
/// metadata comes from the shared codec. A dimension-scaled rounding allowance
/// covers the FP32 scoring kernels in addition to quantization uncertainty.
pub fn distanceInterval(value: []const u8, dims: u32, query: []const f32, measure: f32, query_norm: f32, metric: vectors.DistanceMetric) !struct { lower: f32, upper: f32 } {
    const c = try control(value, dims);
    var projected: [16384]f16 = undefined;
    for (projected[0..dims], 0..) |*v, i| {
        v.* = @bitCast(std.mem.readInt(u16, c.projection[i * 2 ..][0..2], .little));
        if (!std.math.isFinite(v.*)) return error.InvalidVectorValue;
    }
    const estimate = vectors.distanceToQueryF16(query, measure, projected[0..dims], c.scale, metric);
    const interval = vectors.boundedDistanceFromProjectionMetadata(estimate, query_norm, c.error_norm, c.norm_lower, metric);
    const rounding = 32 * @as(f32, @floatFromInt(dims)) * std.math.floatEps(f32);
    const guard = rounding / (1 - rounding) * (@abs(estimate) + 2);
    const lower = interval.distance - interval.error_bound - guard;
    const upper = interval.distance + interval.error_bound + guard;
    return .{ .lower = if (std.math.isFinite(lower)) lower else -std.math.inf(f32), .upper = if (std.math.isFinite(upper)) upper else std.math.inf(f32) };
}

test "serverless compact vector packs reconstruct source bits and conservatively bound every metric" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/blocks", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var backend = fs.artifactStore();
    defer backend.deinit();
    var reads: u64 = 8 * 1024 * 1024;
    var writes: u64 = 8 * 1024 * 1024;
    var pages = PageStore{ .domain = @splat(9), .attempt = @splat(1), .artifacts = &backend, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    var ids: [40][16]u8 = undefined;
    var values: [40][384]f32 = undefined;
    var entries: [40]segment.Entry = undefined;
    var seed: u32 = 17;
    for (&ids, &values, &entries, 0..) |*id, *vector, *entry, i| {
        for (vector) |*v| {
            seed = seed *% 1664525 +% 1013904223;
            v.* = @as(f32, @floatFromInt(seed >> 8)) / 8388608.0 - 1;
        }
        if (i == 0) @memset(vector, 0);
        if (i == 1) vector[0] = -0.0;
        entry.* = .{ .doc_id = try std.fmt.bufPrint(id, "doc/{d}", .{i}), .vector = vector };
    }
    const encoded = try encodeAlloc(a, &pages, &entries);
    defer {
        for (encoded) |bytes| a.free(bytes);
        a.free(encoded);
    }
    var reader = Reader{ .a = a, .pages = &pages };
    defer reader.deinit();
    for (encoded, entries) |bytes, expected| {
        var source = try reader.read(bytes, 384);
        defer source.deinit(a);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected.vector), std.mem.sliceAsBytes(source.vector));
        try std.testing.expectEqualStrings(expected.doc_id, source.doc_id);
        for ([_]vectors.DistanceMetric{ .cosine, .inner_product, .l2_squared }) |metric| {
            const query = &values[19];
            const norm = vectors.norm(query);
            const measure = switch (metric) {
                .cosine => norm,
                .inner_product => 0,
                .l2_squared => vectors.dot(query, query),
            };
            const interval = try distanceInterval(bytes, 384, query, measure, norm, metric);
            const exact = vectors.distanceToQuery(query, measure, expected.vector, metric);
            try std.testing.expect(interval.lower <= exact and exact <= interval.upper);
        }
    }
    const Run = struct {
        fn run(alloc: A, source: *PageStore, value: []const u8) !void {
            var r = Reader{ .a = alloc, .pages = source };
            defer r.deinit();
            var row = try r.read(value, 384);
            defer row.deinit(alloc);
        }
    };
    // Each iteration has independent ownership and independent read admission.
    reads = std.math.maxInt(u64);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Run.run, .{ &pages, encoded[0] });
    reads = 0;
    var warm = try reader.read(encoded[0], 384);
    defer warm.deinit(a);
    const original_domain = pages.domain;
    pages.domain = @splat(10);
    reads = 4 * 1024 * 1024;
    try std.testing.expectError(error.FileNotFound, reader.read(encoded[0], 384));
    pages.domain = original_domain;
    reads = 0;
    var cold = Reader{ .a = a, .pages = &pages };
    defer cold.deinit();
    try std.testing.expectError(error.ArtifactReadBudgetExceeded, cold.read(encoded[0], 384));
    const altered = try a.dupe(u8, encoded[0]);
    defer a.free(altered);
    const data = altered[8..];
    std.mem.writeInt(u32, data[68..72], max_pack_bytes, .little);
    try std.testing.expectError(error.InvalidVectorValue, control(altered, 384));
    const Corrupt = struct {
        fn read(_: *anyopaque, alloc: A, store: *@import("../artifacts/store.zig").ArtifactStore, source: @import("../manifest/mod.zig").ArtifactRef, offset: u64, len: usize, _: [32]u8, token: @import("antfly_cancellation").CancellationToken, remaining: *u64) ![]u8 {
            try @import("../artifacts/store.zig").chargeReadBudget(remaining, len);
            const data_ = try store.getRangeAllocWithCancellationUsingAllocator(alloc, source.artifact_id, offset, len, token);
            data_[0] ^= 1;
            return data_;
        }
    };
    reads = 4 * 1024 * 1024;
    pages.read_cache = .{ .ptr = &backend, .read = Corrupt.read };
    var bad = Reader{ .a = a, .pages = &pages };
    defer bad.deinit();
    try std.testing.expectError(error.ArtifactIntegrityMismatch, bad.read(encoded[0], 384));
    try std.testing.expectEqual(@as(usize, 0), bad.retained);
    try std.testing.expectEqual(@as(usize, 0), bad.slots.items.len);
    var signal = std.atomic.Value(bool).init(true);
    pages.cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&signal);
    try std.testing.expectError(error.Canceled, reader.read(encoded[0], 384));
}
