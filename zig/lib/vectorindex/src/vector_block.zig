// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Immutable, mmap-friendly vector projection blocks.
//!
//! Blocks are table-level projections keyed by the authoritative embedding
//! artifact key, rather than by an index-local vector id. A block may retain
//! multiple source revisions for a key so a query holding an older HBC
//! generation lease cannot accidentally rerank with a newer vector. Primary
//! document/artifact storage remains authoritative; a missing revision is a
//! signal to fall back to that store.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const checked_region = @import("checked_region.zig");

const magic: [8]u8 = .{ 'A', 'F', 'V', 'B', 'L', 'K', 0, 0 };
const legacy_version: u16 = 1;
const unscaled_encoding_version: u16 = 2;
const scaled_encoding_version: u16 = 3;
const version: u16 = 4;
const header_size: usize = 40;
const legacy_index_entry_size: usize = 60;
const scaled_index_entry_size: usize = 64;
const index_entry_size: usize = 88;
const footer_size: usize = 60;
const tombstone_flag: u32 = 1;

pub const Encoding = enum(u16) {
    float32 = 0,
    float16 = 1,

    pub fn componentBytes(self: Encoding) usize {
        return switch (self) {
            .float32 => @sizeOf(f32),
            .float16 => @sizeOf(f16),
        };
    }

    fn alignment(self: Encoding) usize {
        return switch (self) {
            .float32 => @alignOf(f32),
            .float16 => @alignOf(f16),
        };
    }
};

pub fn encodedVectorBytesLen(encoding: Encoding, dims: usize) !usize {
    if (dims == 0) return error.InvalidVectorDimensions;
    return std.math.mul(usize, dims, encoding.componentBytes()) catch error.VectorBlockTooLarge;
}

const residual_code_bits: usize = 13;
const residual_escape_code: u16 = (1 << residual_code_bits) - 1;
const residual_inline_max: u16 = residual_escape_code - 1;

fn residualPackedBytesLen(dims: usize) !usize {
    const bits = std.math.mul(usize, dims, residual_code_bits) catch return error.VectorBlockTooLarge;
    const rounded = std.math.add(usize, bits, 7) catch return error.VectorBlockTooLarge;
    return rounded / 8;
}

/// Maximum scratch space required by `encodeExactResidualInto`. Ordinary
/// normalized embeddings use only 13 bits/component plus the count word; the
/// remaining space is reserved for uncommon values whose IEEE-754 bit delta
/// cannot be represented inline.
pub fn exactResidualMaxBytes(dims: usize) !usize {
    const packed_bytes = try residualPackedBytesLen(dims);
    const exceptions = std.math.mul(usize, dims, 8) catch return error.VectorBlockTooLarge;
    return std.math.add(usize, std.math.add(usize, packed_bytes, 4) catch return error.VectorBlockTooLarge, exceptions) catch
        return error.VectorBlockTooLarge;
}

fn writeResidualCode(packed_bytes: []u8, index: usize, code: u16) void {
    const bit_offset = index * residual_code_bits;
    const byte_offset = bit_offset / 8;
    const shift: u5 = @intCast(bit_offset & 7);
    const wide: u32 = @as(u32, code) << shift;
    packed_bytes[byte_offset] |= @truncate(wide);
    if (byte_offset + 1 < packed_bytes.len) packed_bytes[byte_offset + 1] |= @truncate(wide >> 8);
    if (byte_offset + 2 < packed_bytes.len) packed_bytes[byte_offset + 2] |= @truncate(wide >> 16);
}

fn readResidualCode(packed_bytes: []const u8, index: usize) u16 {
    const bit_offset = index * residual_code_bits;
    const byte_offset = bit_offset / 8;
    const shift: u5 = @intCast(bit_offset & 7);
    var wide: u32 = packed_bytes[byte_offset];
    if (byte_offset + 1 < packed_bytes.len) wide |= @as(u32, packed_bytes[byte_offset + 1]) << 8;
    if (byte_offset + 2 < packed_bytes.len) wide |= @as(u32, packed_bytes[byte_offset + 2]) << 16;
    return @truncate((wide >> shift) & residual_escape_code);
}

fn zigZagEncode(value: i64) u64 {
    return if (value >= 0)
        @as(u64, @intCast(value)) * 2
    else
        @as(u64, @intCast(-value)) * 2 - 1;
}

fn zigZagDecode(value: u16) i64 {
    return if ((value & 1) == 0)
        @intCast(value / 2)
    else
        -@as(i64, @intCast(value / 2)) - 1;
}

fn decodedComponentBits(encoded: []const u8, index: usize, scale: f32) u32 {
    const offset = index * @sizeOf(f16);
    const component: f16 = @bitCast(std.mem.readInt(u16, encoded[offset..][0..2], .little));
    const decoded: f32 = @as(f32, @floatCast(component)) * scale;
    return @bitCast(decoded);
}

/// Encodes the exact difference between a float16 projection and its original
/// float32 vector. Decoding reconstructs every source IEEE-754 bit exactly.
pub fn encodeExactResidualInto(
    source: []const f32,
    encoded: []const u8,
    scale: f32,
    out: []u8,
) !usize {
    try validateEncodedVector(.float16, source.len, encoded, scale);
    const packed_len = try residualPackedBytesLen(source.len);
    const minimum = std.math.add(usize, packed_len, 4) catch return error.VectorBlockTooLarge;
    if (out.len < minimum) return error.BufferTooSmall;
    @memset(out[0..packed_len], 0);
    var exception_count: usize = 0;
    var exception_offset = minimum;
    for (source, 0..) |value, i| {
        if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
        const source_bits: u32 = @bitCast(value);
        const decoded_bits = decodedComponentBits(encoded, i, scale);
        const delta = @as(i64, source_bits) - @as(i64, decoded_bits);
        const code = zigZagEncode(delta);
        if (code <= residual_inline_max) {
            writeResidualCode(out[0..packed_len], i, @intCast(code));
        } else {
            writeResidualCode(out[0..packed_len], i, residual_escape_code);
            if (out.len - exception_offset < 8) return error.BufferTooSmall;
            writeU32(out[exception_offset..][0..4], @intCast(i));
            writeU32(out[exception_offset + 4 ..][0..4], source_bits);
            exception_offset += 8;
            exception_count += 1;
        }
    }
    writeU32(out[packed_len..][0..4], std.math.cast(u32, exception_count) orelse return error.VectorBlockTooLarge);
    return exception_offset;
}

fn validateExactResidual(encoded: []const u8, dims: usize, scale: f32, residual: []const u8) !void {
    const packed_len = try residualPackedBytesLen(dims);
    if (residual.len < packed_len + 4) return error.InvalidExactVectorResidual;
    const exception_count = readU32(residual[packed_len..][0..4]);
    const expected = std.math.add(usize, packed_len + 4, std.math.mul(usize, exception_count, 8) catch
        return error.InvalidExactVectorResidual) catch return error.InvalidExactVectorResidual;
    if (residual.len != expected) return error.InvalidExactVectorResidual;
    var exception_index: usize = 0;
    for (0..dims) |i| {
        const code = readResidualCode(residual[0..packed_len], i);
        var source_bits: u32 = undefined;
        if (code == residual_escape_code) {
            if (exception_index >= exception_count) return error.InvalidExactVectorResidual;
            const offset = packed_len + 4 + exception_index * 8;
            if (readU32(residual[offset..][0..4]) != i) return error.InvalidExactVectorResidual;
            source_bits = readU32(residual[offset + 4 ..][0..4]);
            exception_index += 1;
        } else {
            const source_bits_signed = @as(i64, decodedComponentBits(encoded, i, scale)) + zigZagDecode(code);
            if (source_bits_signed < 0 or source_bits_signed > std.math.maxInt(u32)) return error.InvalidExactVectorResidual;
            source_bits = @intCast(source_bits_signed);
        }
        const value: f32 = @bitCast(source_bits);
        if (!std.math.isFinite(value)) return error.InvalidExactVectorResidual;
    }
    if (exception_index != exception_count) return error.InvalidExactVectorResidual;
}

pub fn decodeExactResidualInto(
    encoded: []const u8,
    dims: usize,
    scale: f32,
    residual: []const u8,
    out: []f32,
) ![]const f32 {
    if (out.len < dims) return error.BufferTooSmall;
    if (encoded.len != try encodedVectorBytesLen(.float16, dims)) return error.InvalidEncodedVectorLength;
    if (!std.math.isFinite(scale) or scale <= 0) return error.InvalidVectorScale;
    const packed_len = try residualPackedBytesLen(dims);
    if (residual.len < packed_len + 4) return error.InvalidExactVectorResidual;
    const exception_count = readU32(residual[packed_len..][0..4]);
    const expected = std.math.add(usize, packed_len + 4, std.math.mul(usize, exception_count, 8) catch
        return error.InvalidExactVectorResidual) catch return error.InvalidExactVectorResidual;
    if (residual.len != expected) return error.InvalidExactVectorResidual;
    var exception_index: usize = 0;
    for (out[0..dims], 0..) |*value, i| {
        const code = readResidualCode(residual[0..packed_len], i);
        const source_bits: u32 = if (code == residual_escape_code) blk: {
            if (exception_index >= exception_count) return error.InvalidExactVectorResidual;
            const offset = packed_len + 4 + exception_index * 8;
            if (readU32(residual[offset..][0..4]) != i) return error.InvalidExactVectorResidual;
            exception_index += 1;
            break :blk readU32(residual[offset + 4 ..][0..4]);
        } else blk: {
            const source_bits_signed = @as(i64, decodedComponentBits(encoded, i, scale)) + zigZagDecode(code);
            if (source_bits_signed < 0 or source_bits_signed > std.math.maxInt(u32)) return error.InvalidExactVectorResidual;
            break :blk @intCast(source_bits_signed);
        };
        value.* = @bitCast(source_bits);
        if (!std.math.isFinite(value.*)) return error.InvalidExactVectorResidual;
    }
    if (exception_index != exception_count) return error.InvalidExactVectorResidual;
    return out[0..dims];
}

/// Encodes one exact-vector payload in the same representation stored by a
/// block. The returned scale must be persisted beside `out`; float16 blocks
/// use it to retain vectors whose components exceed the native f16 range.
pub const QuantizationStats = struct {
    /// Conservative upper bound on ||source_f32 - decoded_projection||₂.
    error_norm: f32,
    /// Conservative lower bound on ||decoded_projection||₂.
    decoded_norm_lower_bound: f32,
};

pub const EncodedVectorStats = struct {
    scale: f32,
    quantization: QuantizationStats,
};

fn upperRoundedNorm(sum_squared: f64) f32 {
    const value: f32 = @floatCast(@sqrt(sum_squared));
    return value + 8.0 * std.math.floatEps(f32) * (value + 1.0);
}

fn lowerRoundedNorm(sum_squared: f64) f32 {
    const value: f32 = @floatCast(@sqrt(sum_squared));
    return @max(0, value - 8.0 * std.math.floatEps(f32) * (value + 1.0));
}

fn upperRoundedNormF32(sum_squared: f32, dims: usize) f32 {
    const value = @sqrt(sum_squared);
    // Each component contributes one rounded multiply and one rounded add.
    // Inflate beyond the standard positive-sum gamma bound, then take the
    // square root. Supported vector dimensions keep this well below 1.
    const relative_slop = @min(0.25, 16.0 * std.math.floatEps(f32) * @as(f32, @floatFromInt(dims)));
    return value * (1.0 + relative_slop);
}

fn lowerRoundedNormF32(sum_squared: f32, dims: usize) f32 {
    const value = @sqrt(sum_squared);
    const relative_slop = @min(0.25, 16.0 * std.math.floatEps(f32) * @as(f32, @floatFromInt(dims)));
    return value * (1.0 - relative_slop);
}

fn sourceNormSquaredF32(values: []const f32) f32 {
    const Simd = @Vector(8, f32);
    var sum0: Simd = @splat(0);
    var sum1: Simd = @splat(0);
    var sum2: Simd = @splat(0);
    var sum3: Simd = @splat(0);
    var i: usize = 0;
    while (i + 32 <= values.len) : (i += 32) {
        const v0: Simd = values[i..][0..8].*;
        const v1: Simd = values[i + 8 ..][0..8].*;
        const v2: Simd = values[i + 16 ..][0..8].*;
        const v3: Simd = values[i + 24 ..][0..8].*;
        sum0 += v0 * v0;
        sum1 += v1 * v1;
        sum2 += v2 * v2;
        sum3 += v3 * v3;
    }
    var sum = @reduce(.Add, sum0 + sum1 + sum2 + sum3);
    while (i < values.len) : (i += 1) sum += values[i] * values[i];
    return sum;
}

pub fn encodedQuantizationStats(
    encoding: Encoding,
    bytes: []const u8,
    dims: usize,
    scale: f32,
) !QuantizationStats {
    try validateEncodedVector(encoding, dims, bytes, scale);
    var error_norm_squared: f64 = 0;
    var decoded_norm_squared: f64 = 0;
    var pos: usize = 0;
    for (0..dims) |_| switch (encoding) {
        .float32 => {
            const decoded: f32 = @bitCast(std.mem.readInt(u32, bytes[pos..][0..4], .little));
            decoded_norm_squared += @as(f64, decoded) * decoded;
            pos += 4;
        },
        .float16 => {
            const component_f16: f16 = @bitCast(std.mem.readInt(u16, bytes[pos..][0..2], .little));
            const component: f32 = @floatCast(component_f16);
            const decoded = component * scale;
            // A full f16 ULP also covers the f32 divide/multiply round trips
            // used by the encoder. This path is used for legacy/compacted
            // encoded input whose original f32 values are unavailable.
            const component_error = scale *
                (@abs(component) * (1.0 / 1024.0) + 1.0 / 8_388_608.0);
            error_norm_squared += @as(f64, component_error) * component_error;
            decoded_norm_squared += @as(f64, decoded) * decoded;
            pos += 2;
        },
    };
    return .{
        .error_norm = upperRoundedNorm(error_norm_squared),
        .decoded_norm_lower_bound = lowerRoundedNorm(decoded_norm_squared),
    };
}

pub fn encodeVectorIntoWithStats(encoding: Encoding, vector: []const f32, out: []u8) !EncodedVectorStats {
    const expected = try encodedVectorBytesLen(encoding, vector.len);
    if (out.len != expected) return error.InvalidEncodedVectorLength;

    var scale: f32 = 1;
    if (encoding == .float16) {
        var max_abs: f32 = 0;
        // Validate while computing the scale so float16 encoding needs only
        // two source traversals (scale and conversion), not three.
        for (vector) |value| {
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            max_abs = @max(max_abs, @abs(value));
        }
        // Leave conversion headroom below the largest finite f16 so f32
        // division rounding cannot turn the extremum into inf.
        if (max_abs > 65_000.0) scale = max_abs / 65_000.0;
    }
    var pos: usize = 0;
    for (vector) |value| switch (encoding) {
        .float32 => {
            // float32 needs only this single validation/encoding traversal.
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            std.mem.writeInt(u32, out[pos..][0..4], @bitCast(value), .little);
            pos += 4;
        },
        .float16 => {
            const encoded: f16 = @floatCast(value / scale);
            std.mem.writeInt(u16, out[pos..][0..2], @bitCast(encoded), .little);
            pos += 2;
        },
    };
    const source_norm_squared = sourceNormSquaredF32(vector);
    const quantization: QuantizationStats = switch (encoding) {
        .float32 => QuantizationStats{
            .error_norm = 0,
            .decoded_norm_lower_bound = if (std.math.isFinite(source_norm_squared))
                lowerRoundedNormF32(source_norm_squared, vector.len)
            else
                std.math.floatMax(f32),
        },
        .float16 => blk: {
            // Round-to-nearest f16 error is bounded from the source norm
            // without revisiting the encoded payload. The 1/1023 factor is
            // the conservative solution of e <= decoded/1024 + subnormal
            // allowance with decoded <= source + e.
            if (!std.math.isFinite(source_norm_squared))
                break :blk try encodedQuantizationStats(encoding, out, vector.len, scale);
            const source_norm_upper = upperRoundedNormF32(source_norm_squared, vector.len);
            const source_norm_lower = lowerRoundedNormF32(source_norm_squared, vector.len);
            const subnormal_error = @sqrt(@as(f32, @floatFromInt(vector.len))) * scale *
                (1024.0 / 1023.0) * (1.0 / 8_388_608.0);
            const error_norm = source_norm_upper / 1023.0 + subnormal_error;
            break :blk .{
                .error_norm = error_norm,
                .decoded_norm_lower_bound = @max(0, source_norm_lower - error_norm),
            };
        },
    };
    return .{
        .scale = scale,
        .quantization = quantization,
    };
}

pub fn encodeVectorInto(encoding: Encoding, vector: []const f32, out: []u8) !f32 {
    return (try encodeVectorIntoWithStats(encoding, vector, out)).scale;
}

fn validateEncodedVector(encoding: Encoding, dims: usize, bytes: []const u8, scale: f32) !void {
    if (bytes.len != try encodedVectorBytesLen(encoding, dims)) return error.InvalidEncodedVectorLength;
    if (!std.math.isFinite(scale) or scale <= 0) return error.InvalidVectorScale;
    var pos: usize = 0;
    for (0..dims) |_| switch (encoding) {
        .float32 => {
            const value: f32 = @bitCast(std.mem.readInt(u32, bytes[pos..][0..4], .little));
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            pos += 4;
        },
        .float16 => {
            const value: f16 = @bitCast(std.mem.readInt(u16, bytes[pos..][0..2], .little));
            if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
            pos += 2;
        },
    };
}

fn validateQuantizationStats(encoding: Encoding, stats: QuantizationStats) !void {
    if (!std.math.isFinite(stats.error_norm) or stats.error_norm < 0 or
        !std.math.isFinite(stats.decoded_norm_lower_bound) or stats.decoded_norm_lower_bound < 0)
        return error.InvalidVectorQuantizationStats;
    if (encoding == .float32 and stats.error_norm != 0)
        return error.InvalidVectorQuantizationStats;
}

pub fn keyHash(key: []const u8) u64 {
    return std.hash.XxHash64.hash(0, key);
}

pub fn shardForKey(key: []const u8, shard_count: u32) !u32 {
    if (!validShardCount(shard_count)) return error.InvalidVectorBlockShardCount;
    return @intCast(keyHash(key) & (@as(u64, shard_count) - 1));
}

pub const Writer = struct {
    alloc: Allocator,
    // Keep keys and vector payloads in separate physical regions. Reader
    // admission validates every key to prove hash-shard membership and total
    // ordering before binary search is allowed. Interleaving each key with a
    // multi-KiB vector made that compact validation fault nearly the entire
    // mmap into RSS. New blocks retain keys in `data` and stage vectors here;
    // build() fixes their relative offsets after publishing the aligned vector
    // arena. The on-disk reader contract remains compatible with older blocks.
    vector_data: std.ArrayListUnmanaged(u8) = .empty,
    residual_data: std.ArrayListUnmanaged(u8) = .empty,
    data: std.ArrayListUnmanaged(u8) = .empty,
    index: std.ArrayListUnmanaged(u8) = .empty,
    generation: u64,
    shard_id: u32,
    shard_count: u32,
    covered_source_sequence: u64,
    encoding: Encoding,
    count: u64 = 0,
    previous: ?OrderingKey = null,
    finished: bool = false,

    const OrderingKey = struct {
        hash: u64,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
    };

    const VectorPayload = union(enum) {
        decoded: []const f32,
        encoded: struct {
            bytes: []const u8,
            scale: f32,
            quantization: ?QuantizationStats = null,
            exact_residual: ?[]const u8 = null,
        },
    };

    pub fn init(
        alloc: Allocator,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        covered_source_sequence: u64,
    ) !Writer {
        return try initWithEncoding(alloc, generation, shard_id, shard_count, covered_source_sequence, .float32);
    }

    pub fn initWithEncoding(
        alloc: Allocator,
        generation: u64,
        shard_id: u32,
        shard_count: u32,
        covered_source_sequence: u64,
        encoding: Encoding,
    ) !Writer {
        if (!validShardCount(shard_count)) return error.InvalidVectorBlockShardCount;
        if (shard_id >= shard_count) return error.InvalidVectorBlockShard;
        var self: Writer = .{
            .alloc = alloc,
            .generation = generation,
            .shard_id = shard_id,
            .shard_count = shard_count,
            .covered_source_sequence = covered_source_sequence,
            .encoding = encoding,
        };
        errdefer self.deinit();
        try self.data.resize(alloc, header_size);
        @memcpy(self.data.items[0..magic.len], &magic);
        writeU16(self.data.items[8..10], version);
        writeU16(self.data.items[10..12], @intFromEnum(encoding));
        writeU64(self.data.items[12..20], generation);
        writeU32(self.data.items[20..24], shard_id);
        writeU32(self.data.items[24..28], shard_count);
        writeU64(self.data.items[28..36], covered_source_sequence);
        writeU32(self.data.items[36..40], std.hash.Crc32.hash(self.data.items[0..36]));
        return self;
    }

    pub fn deinit(self: *Writer) void {
        self.vector_data.deinit(self.alloc);
        self.residual_data.deinit(self.alloc);
        self.data.deinit(self.alloc);
        self.index.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn appendVector(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        vector: []const f32,
    ) !void {
        if (vector.len == 0) return error.InvalidVectorDimensions;
        for (vector) |value| if (!std.math.isFinite(value)) return error.InvalidVectorComponent;
        const dims = std.math.cast(u32, vector.len) orelse return error.VectorBlockTooLarge;
        try self.append(key, source_sequence, revision, dims, .{ .decoded = vector }, false);
    }

    /// Appends bytes produced by `encodeVectorInto` without decoding and
    /// encoding them again. This is used by bounded bulk builders that spool
    /// the target representation directly.
    pub fn appendEncodedVector(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        dims: u32,
        bytes: []const u8,
        scale: f32,
    ) !void {
        try validateEncodedVector(self.encoding, dims, bytes, scale);
        try self.append(key, source_sequence, revision, dims, .{ .encoded = .{
            .bytes = bytes,
            .scale = scale,
            .quantization = null,
            .exact_residual = null,
        } }, false);
    }

    pub fn appendEncodedVectorWithStats(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        dims: u32,
        bytes: []const u8,
        scale: f32,
        quantization: QuantizationStats,
    ) !void {
        try validateEncodedVector(self.encoding, dims, bytes, scale);
        try validateQuantizationStats(self.encoding, quantization);
        try self.append(key, source_sequence, revision, dims, .{ .encoded = .{
            .bytes = bytes,
            .scale = scale,
            .quantization = quantization,
            .exact_residual = null,
        } }, false);
    }

    pub fn appendEncodedVectorWithStatsAndResidual(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        dims: u32,
        bytes: []const u8,
        scale: f32,
        quantization: QuantizationStats,
        exact_residual: []const u8,
    ) !void {
        try validateEncodedVector(self.encoding, dims, bytes, scale);
        try validateQuantizationStats(self.encoding, quantization);
        if (self.encoding != .float16) return error.UnexpectedExactVectorResidual;
        try validateExactResidual(bytes, dims, scale, exact_residual);
        try self.append(key, source_sequence, revision, dims, .{ .encoded = .{
            .bytes = bytes,
            .scale = scale,
            .quantization = quantization,
            .exact_residual = exact_residual,
        } }, false);
    }

    pub fn appendTombstone(self: *Writer, key: []const u8, source_sequence: u64, revision: u64) !void {
        try self.append(key, source_sequence, revision, 0, .{ .encoded = .{
            .bytes = &.{},
            .scale = 0,
            .quantization = null,
            .exact_residual = null,
        } }, true);
    }

    fn append(
        self: *Writer,
        key: []const u8,
        source_sequence: u64,
        revision: u64,
        dims: u32,
        payload: VectorPayload,
        tombstone: bool,
    ) !void {
        if (self.finished) return error.VectorBlockWriterFinished;
        if (key.len == 0) return error.InvalidVectorBlockKey;
        if (source_sequence > self.covered_source_sequence) return error.VectorBlockSequenceBeyondCoverage;
        const key_len = std.math.cast(u32, key.len) orelse return error.VectorBlockTooLarge;
        const hash = keyHash(key);
        if ((hash & (@as(u64, self.shard_count) - 1)) != self.shard_id) return error.WrongVectorBlockShard;
        const ordering: OrderingKey = .{
            .hash = hash,
            .key = key,
            .source_sequence = source_sequence,
            .revision = revision,
        };
        if (self.previous) |previous| {
            if (compareOrdering(previous, ordering) != .lt) return error.OutOfOrderVectorBlockEntry;
        }

        const key_offset = self.data.items.len;
        try self.data.appendSlice(self.alloc, key);
        var vector_offset: usize = 0;
        var vector_checksum: u32 = 0;
        var vector_scale: f32 = 1;
        var residual_offset: usize = 0;
        var residual_len: usize = 0;
        var residual_checksum: u32 = 0;
        var quantization: QuantizationStats = .{
            .error_norm = 0,
            .decoded_norm_lower_bound = 0,
        };
        if (!tombstone) {
            const padding = std.mem.alignForward(usize, self.vector_data.items.len, self.encoding.alignment()) - self.vector_data.items.len;
            try self.vector_data.appendNTimes(self.alloc, 0, padding);
            // This is relative to the vector arena until build() knows its
            // aligned absolute offset.
            vector_offset = self.vector_data.items.len;
            const vector_bytes = try encodedVectorBytesLen(self.encoding, dims);
            const old_len = self.vector_data.items.len;
            try self.vector_data.resize(self.alloc, std.math.add(usize, old_len, vector_bytes) catch return error.VectorBlockTooLarge);
            const destination = self.vector_data.items[old_len..][0..vector_bytes];
            switch (payload) {
                .decoded => |vector| {
                    const encoded = try encodeVectorIntoWithStats(self.encoding, vector, destination);
                    vector_scale = encoded.scale;
                    quantization = encoded.quantization;
                    if (self.encoding == .float16) {
                        residual_offset = self.residual_data.items.len;
                        const max_len = try exactResidualMaxBytes(dims);
                        try self.residual_data.resize(self.alloc, std.math.add(usize, residual_offset, max_len) catch return error.VectorBlockTooLarge);
                        residual_len = try encodeExactResidualInto(
                            vector,
                            destination,
                            vector_scale,
                            self.residual_data.items[residual_offset..][0..max_len],
                        );
                        self.residual_data.shrinkRetainingCapacity(residual_offset + residual_len);
                        residual_checksum = std.hash.Crc32.hash(self.residual_data.items[residual_offset..][0..residual_len]);
                    }
                },
                .encoded => |encoded| {
                    try validateEncodedVector(self.encoding, dims, encoded.bytes, encoded.scale);
                    @memcpy(destination, encoded.bytes);
                    vector_scale = encoded.scale;
                    quantization = encoded.quantization orelse try encodedQuantizationStats(
                        self.encoding,
                        encoded.bytes,
                        dims,
                        encoded.scale,
                    );
                    if (encoded.exact_residual) |residual| {
                        if (self.encoding != .float16) return error.UnexpectedExactVectorResidual;
                        try validateExactResidual(destination, dims, vector_scale, residual);
                        residual_offset = self.residual_data.items.len;
                        try self.residual_data.appendSlice(self.alloc, residual);
                        residual_len = residual.len;
                        residual_checksum = std.hash.Crc32.hash(residual);
                    }
                },
            }
            vector_checksum = std.hash.Crc32.hash(destination);
        }

        try appendU64(self.alloc, &self.index, hash);
        try appendU64(self.alloc, &self.index, @intCast(key_offset));
        try appendU32(self.alloc, &self.index, key_len);
        try appendU32(self.alloc, &self.index, std.hash.Crc32.hash(key));
        try appendU64(self.alloc, &self.index, source_sequence);
        try appendU64(self.alloc, &self.index, revision);
        try appendU64(self.alloc, &self.index, @intCast(vector_offset));
        try appendU32(self.alloc, &self.index, dims);
        try appendU32(self.alloc, &self.index, if (tombstone) tombstone_flag else 0);
        try appendU32(self.alloc, &self.index, vector_checksum);
        try appendU32(self.alloc, &self.index, if (tombstone) 0 else @bitCast(vector_scale));
        try appendU32(self.alloc, &self.index, if (tombstone) 0 else @bitCast(quantization.error_norm));
        try appendU32(self.alloc, &self.index, if (tombstone) 0 else @bitCast(quantization.decoded_norm_lower_bound));
        try appendU64(self.alloc, &self.index, @intCast(residual_offset));
        try appendU32(self.alloc, &self.index, @intCast(residual_len));
        try appendU32(self.alloc, &self.index, residual_checksum);
        self.previous = ordering;
        self.count += 1;
    }

    pub fn build(self: *Writer) ![]u8 {
        if (self.finished) return error.VectorBlockWriterFinished;
        self.finished = true;

        const vector_arena_offset = std.mem.alignForward(usize, self.data.items.len, self.encoding.alignment());
        try self.data.appendNTimes(self.alloc, 0, vector_arena_offset - self.data.items.len);
        try self.data.appendSlice(self.alloc, self.vector_data.items);

        const residual_arena_offset = self.data.items.len;
        try self.data.appendSlice(self.alloc, self.residual_data.items);

        // Index vector offsets were recorded relative to vector_data so keys
        // could remain contiguous. Convert only live entries; tombstones keep
        // the required all-zero vector tuple.
        var entry_offset: usize = 0;
        while (entry_offset < self.index.items.len) : (entry_offset += index_entry_size) {
            const flags = readU32(self.index.items[entry_offset + 52 ..][0..4]);
            if ((flags & tombstone_flag) != 0) continue;
            const relative = std.math.cast(usize, readU64(self.index.items[entry_offset + 40 ..][0..8])) orelse return error.VectorBlockTooLarge;
            const absolute = std.math.add(usize, vector_arena_offset, relative) catch return error.VectorBlockTooLarge;
            writeU64(self.index.items[entry_offset + 40 ..][0..8], absolute);
            const residual_len = readU32(self.index.items[entry_offset + 80 ..][0..4]);
            if (residual_len != 0) {
                const residual_relative = std.math.cast(usize, readU64(self.index.items[entry_offset + 72 ..][0..8])) orelse return error.VectorBlockTooLarge;
                const residual_absolute = std.math.add(usize, residual_arena_offset, residual_relative) catch return error.VectorBlockTooLarge;
                writeU64(self.index.items[entry_offset + 72 ..][0..8], residual_absolute);
            }
        }

        const index_offset = self.data.items.len;
        try self.data.appendSlice(self.alloc, self.index.items);
        const footer_start = self.data.items.len;
        try appendU64(self.alloc, &self.data, @intCast(index_offset));
        try appendU64(self.alloc, &self.data, self.count);
        try appendU64(self.alloc, &self.data, self.covered_source_sequence);
        try appendU64(self.alloc, &self.data, self.generation);
        try appendU32(self.alloc, &self.data, std.hash.Crc32.hash(self.index.items));
        try appendU16(self.alloc, &self.data, version);
        try appendU16(self.alloc, &self.data, @intFromEnum(self.encoding));
        try appendU32(self.alloc, &self.data, self.shard_id);
        try appendU32(self.alloc, &self.data, self.shard_count);
        try appendU32(self.alloc, &self.data, std.hash.Crc32.hash(self.data.items[footer_start..][0..48]));
        try self.data.appendSlice(self.alloc, &magic);
        self.vector_data.clearAndFree(self.alloc);
        self.residual_data.clearAndFree(self.alloc);
        self.index.clearAndFree(self.alloc);
        return try self.data.toOwnedSlice(self.alloc);
    }
};

pub const Value = struct {
    source_sequence: u64,
    revision: u64,
    dims: u32,
    bytes: []const u8,
    encoding: Encoding = .float32,
    scale: f32 = 1,
    quantization_error_norm: ?f32 = null,
    decoded_norm_lower_bound: ?f32 = null,
    exact_residual: ?[]const u8 = null,

    pub fn vectorView(self: Value) ?[]const f32 {
        if (self.encoding != .float32 or self.scale != 1) return null;
        if (builtin.target.cpu.arch.endian() != .little) return null;
        if (@intFromPtr(self.bytes.ptr) % @alignOf(f32) != 0) return null;
        const aligned: []align(@alignOf(f32)) const u8 = @alignCast(self.bytes);
        return std.mem.bytesAsSlice(f32, aligned);
    }

    pub fn decodeInto(self: Value, out: []f32) ![]const f32 {
        if (self.dims > out.len) return error.BufferTooSmall;
        if (self.vectorView()) |view| {
            @memcpy(out[0..self.dims], view);
            return out[0..self.dims];
        }
        var pos: usize = 0;
        for (out[0..self.dims]) |*value| {
            switch (self.encoding) {
                .float32 => {
                    value.* = @bitCast(std.mem.readInt(u32, self.bytes[pos..][0..4], .little));
                    pos += 4;
                },
                .float16 => {
                    const encoded: f16 = @bitCast(std.mem.readInt(u16, self.bytes[pos..][0..2], .little));
                    value.* = @as(f32, @floatCast(encoded)) * self.scale;
                    pos += 2;
                },
            }
        }
        return out[0..self.dims];
    }

    /// Reconstructs the authoritative source float32 vector. Float32 blocks
    /// already contain that plane; float16 blocks require a complete lossless
    /// residual published in the same immutable generation.
    pub fn decodeExactInto(self: Value, out: []f32) ![]const f32 {
        if (self.encoding == .float32) return self.decodeInto(out);
        const residual = self.exact_residual orelse return error.ExactVectorResidualMissing;
        return decodeExactResidualInto(self.bytes, self.dims, self.scale, residual, out);
    }
};

/// Immutable coordinates and scoring metadata for one vector payload. This
/// lets storage owners binary-search mmap metadata while fetching the large
/// vector/residual planes with bounded positional I/O.
pub const ValueLocation = struct {
    source_sequence: u64,
    revision: u64,
    dims: u32,
    encoding: Encoding,
    scale: f32,
    quantization_error_norm: ?f32,
    decoded_norm_lower_bound: ?f32,
    vector_offset: usize,
    vector_len: usize,
    vector_checksum: u32,
    residual_offset: usize,
    residual_len: usize,
    residual_checksum: u32,

    pub fn scratchBytes(self: ValueLocation) !usize {
        return std.math.add(usize, self.vector_len, self.residual_len) catch error.VectorBlockTooLarge;
    }

    /// Validates and exposes only the compact candidate plane. The exact
    /// residual deliberately remains unread so bounded scoring does not turn
    /// every RaBitQ boundary candidate into an exact-vector I/O operation.
    pub fn projectionValueFromPayload(self: ValueLocation, vector_bytes: []const u8) !Value {
        if (vector_bytes.len != self.vector_len) return error.CorruptedVectorBlock;
        if (std.hash.Crc32.hash(vector_bytes) != self.vector_checksum)
            return error.VectorBlockPayloadChecksumMismatch;
        return self.projectionValueFromVerifiedPayload(vector_bytes, self.vector_checksum);
    }

    /// Exposes bytes authenticated by another immutable container. The
    /// caller supplies that container's persisted payload checksum; matching
    /// it to this authoritative location avoids hashing the same projection
    /// again on every exact-completion query.
    pub fn projectionValueFromVerifiedPayload(
        self: ValueLocation,
        vector_bytes: []const u8,
        verified_checksum: u32,
    ) !Value {
        if (vector_bytes.len != self.vector_len) return error.CorruptedVectorBlock;
        if (verified_checksum != self.vector_checksum) return error.VectorBlockPayloadChecksumMismatch;
        return .{
            .source_sequence = self.source_sequence,
            .revision = self.revision,
            .dims = self.dims,
            .bytes = vector_bytes,
            .encoding = self.encoding,
            .scale = self.scale,
            .quantization_error_norm = self.quantization_error_norm,
            .decoded_norm_lower_bound = self.decoded_norm_lower_bound,
            .exact_residual = null,
        };
    }

    pub fn valueFromPayload(self: ValueLocation, vector_bytes: []const u8, residual_bytes: []const u8) !Value {
        const value = try self.projectionValueFromPayload(vector_bytes);
        return try self.completeProjection(value, residual_bytes);
    }

    /// Attaches the lossless plane to a projection that this location already
    /// validated. Query rerank keeps the compact bytes generation-leased after
    /// candidate scoring, so exact boundary completion should validate and
    /// touch only the residual rather than hashing the projection twice.
    pub fn completeProjection(self: ValueLocation, projection: Value, residual_bytes: []const u8) !Value {
        if (projection.source_sequence != self.source_sequence or
            projection.revision != self.revision or
            projection.dims != self.dims or
            projection.encoding != self.encoding or
            @as(u32, @bitCast(projection.scale)) != @as(u32, @bitCast(self.scale)) or
            projection.quantization_error_norm != self.quantization_error_norm or
            projection.decoded_norm_lower_bound != self.decoded_norm_lower_bound or
            projection.bytes.len != self.vector_len or
            projection.exact_residual != null)
        {
            return error.VectorBlockProjectionLocationMismatch;
        }
        if (residual_bytes.len != self.residual_len) return error.CorruptedVectorBlock;
        if (self.residual_len != 0 and std.hash.Crc32.hash(residual_bytes) != self.residual_checksum)
            return error.VectorBlockResidualChecksumMismatch;
        var value = projection;
        value.exact_residual = if (residual_bytes.len == 0) null else residual_bytes;
        return value;
    }
};

pub const Tombstone = struct {
    source_sequence: u64,
    revision: u64,
};

pub const Lookup = union(enum) {
    missing,
    tombstone: Tombstone,
    vector: Value,
};

pub const LocatedLookup = union(enum) {
    missing,
    tombstone: Tombstone,
    vector: ValueLocation,
};

/// Borrowed, checksum-verified view used by bounded generation compaction.
/// The key and vector bytes remain owned by the reader's mmap lease.
pub const EntryView = struct {
    key: []const u8,
    value: Lookup,
};

pub const Reader = struct {
    data: []const u8,
    index_offset: usize,
    count: usize,
    generation: u64,
    shard_id: u32,
    shard_count: u32,
    covered_source_sequence: u64,
    encoding: Encoding,
    index_entry_size: usize,

    const Entry = struct {
        hash: u64,
        key_offset: usize,
        key_len: usize,
        key_checksum: u32,
        source_sequence: u64,
        revision: u64,
        vector_offset: usize,
        dims: u32,
        flags: u32,
        vector_checksum: u32,
        vector_scale: f32,
        quantization_error_norm: ?f32,
        decoded_norm_lower_bound: ?f32,
        residual_offset: usize,
        residual_len: usize,
        residual_checksum: u32,
    };

    pub fn init(data: []const u8) !Reader {
        if (data.len < header_size + footer_size) return error.CorruptedVectorBlock;
        if (!std.mem.eql(u8, data[0..8], &magic)) return error.CorruptedVectorBlock;
        const block_version = readU16(data[8..10]);
        const encoding: Encoding = switch (block_version) {
            legacy_version => if (readU16(data[10..12]) == 0) .float32 else return error.UnsupportedVectorBlockVersion,
            unscaled_encoding_version, scaled_encoding_version, version => switch (readU16(data[10..12])) {
                @intFromEnum(Encoding.float32) => .float32,
                @intFromEnum(Encoding.float16) => .float16,
                else => return error.UnsupportedVectorBlockEncoding,
            },
            else => return error.UnsupportedVectorBlockVersion,
        };
        const entry_size = switch (block_version) {
            version => index_entry_size,
            scaled_encoding_version => scaled_index_entry_size,
            legacy_version, unscaled_encoding_version => legacy_index_entry_size,
            else => unreachable,
        };
        if (readU32(data[36..40]) != std.hash.Crc32.hash(data[0..36])) return error.VectorBlockHeaderChecksumMismatch;

        const generation = readU64(data[12..20]);
        const shard_id = readU32(data[20..24]);
        const shard_count = readU32(data[24..28]);
        const covered_source_sequence = readU64(data[28..36]);
        if (!validShardCount(shard_count) or shard_id >= shard_count) return error.CorruptedVectorBlock;
        const footer = data[data.len - footer_size ..];
        if (!std.mem.eql(u8, footer[52..60], &magic)) return error.CorruptedVectorBlock;
        if (readU16(footer[36..38]) != block_version or readU16(footer[38..40]) != @intFromEnum(encoding)) return error.UnsupportedVectorBlockVersion;
        if (readU32(footer[48..52]) != std.hash.Crc32.hash(footer[0..48])) return error.VectorBlockFooterChecksumMismatch;
        if (readU64(footer[24..32]) != generation) return error.CorruptedVectorBlock;
        if (readU32(footer[40..44]) != shard_id or readU32(footer[44..48]) != shard_count) return error.CorruptedVectorBlock;
        if (readU64(footer[16..24]) != covered_source_sequence) return error.CorruptedVectorBlock;
        const count_raw = readU64(footer[8..16]);
        const index_region = checked_region.exactTail(
            data.len,
            footer_size,
            header_size,
            readU64(footer[0..8]),
            count_raw,
            entry_size,
        ) catch return error.CorruptedVectorBlock;
        const count = std.math.cast(usize, count_raw) orelse return error.CorruptedVectorBlock;
        if (readU32(footer[32..36]) != std.hash.Crc32.hash(index_region.slice(data))) return error.VectorBlockIndexChecksumMismatch;
        const reader: Reader = .{
            .data = data,
            .index_offset = index_region.offset,
            .count = count,
            .generation = generation,
            .shard_id = shard_id,
            .shard_count = shard_count,
            .covered_source_sequence = covered_source_sequence,
            .encoding = encoding,
            .index_entry_size = entry_size,
        };
        try reader.validate();
        return reader;
    }

    /// Cheap identity checksum for a manifest descriptor. Entry payloads keep
    /// their own lazy checksums; this binds the validated header, footer, and
    /// index checksum without rereading the potentially multi-GiB vector data.
    pub fn admissionChecksum(self: Reader) u32 {
        var identity: [12]u8 = undefined;
        writeU32(identity[0..4], readU32(self.data[36..40]));
        writeU32(identity[4..8], readU32(self.data[self.data.len - footer_size + 32 ..][0..4]));
        writeU32(identity[8..12], readU32(self.data[self.data.len - footer_size + 48 ..][0..4]));
        return std.hash.Crc32.hash(&identity);
    }

    pub fn get(self: Reader, key: []const u8, max_source_sequence: u64, expected_revision: ?u64) !Lookup {
        const hash = keyHash(key);
        return self.getHashed(key, hash, max_source_sequence, expected_revision);
    }

    /// Point lookup using a caller-provided hash. Reader admission has already
    /// verified the complete immutable index, every entry invariant, ordering,
    /// and every key checksum. Query lookups can therefore trust those regions
    /// for the lifetime of the mmap lease while payload CRCs remain lazy.
    pub fn getHashed(self: Reader, key: []const u8, hash: u64, max_source_sequence: u64, expected_revision: ?u64) !Lookup {
        return switch (try self.locateHashed(key, hash, max_source_sequence, expected_revision)) {
            .missing => .missing,
            .tombstone => |value| .{ .tombstone = value },
            .vector => |location| .{ .vector = try location.valueFromPayload(
                self.data[location.vector_offset..][0..location.vector_len],
                if (location.residual_len == 0)
                    &.{}
                else
                    self.data[location.residual_offset..][0..location.residual_len],
            ) },
        };
    }

    /// Performs the complete checked key/revision lookup without touching the
    /// vector arena. Storage layers can use the returned offsets with pread.
    pub fn locateHashed(self: Reader, key: []const u8, hash: u64, max_source_sequence: u64, expected_revision: ?u64) !LocatedLookup {
        if ((hash & (@as(u64, self.shard_count) - 1)) != self.shard_id) return .missing;
        var lo: usize = 0;
        var hi = self.count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const found = self.entryAssumeValidated(mid);
            if (self.compareEntryKeyAssumeValidated(found, hash, key) == .lt) lo = mid + 1 else hi = mid;
        }

        var selected: ?Entry = null;
        var pos = lo;
        while (pos < self.count) : (pos += 1) {
            const candidate = self.entryAssumeValidated(pos);
            if (self.compareEntryKeyAssumeValidated(candidate, hash, key) != .eq) break;
            if (candidate.source_sequence > max_source_sequence) break;
            selected = candidate;
        }
        const found = selected orelse return .missing;
        if (expected_revision) |expected| {
            if (found.revision != expected) return error.VectorBlockRevisionMismatch;
        }
        if ((found.flags & tombstone_flag) != 0) return .{ .tombstone = .{
            .source_sequence = found.source_sequence,
            .revision = found.revision,
        } };
        const vector_len = std.math.mul(usize, found.dims, self.encoding.componentBytes()) catch
            return error.CorruptedVectorBlock;
        return .{ .vector = .{
            .source_sequence = found.source_sequence,
            .revision = found.revision,
            .dims = found.dims,
            .encoding = self.encoding,
            .scale = found.vector_scale,
            .quantization_error_norm = found.quantization_error_norm,
            .decoded_norm_lower_bound = found.decoded_norm_lower_bound,
            .vector_offset = found.vector_offset,
            .vector_len = vector_len,
            .vector_checksum = found.vector_checksum,
            .residual_offset = found.residual_offset,
            .residual_len = found.residual_len,
            .residual_checksum = found.residual_checksum,
        } };
    }

    /// Returns one physical entry in block sort order. This deliberately does
    /// not expose the private index representation: callers receive the same
    /// lazy key/payload checksum guarantees as point lookup while retaining a
    /// zero-copy mmap view for merge compaction.
    pub fn entryAt(self: Reader, index: usize) !EntryView {
        const found = try self.entry(index);
        return .{
            .key = try self.entryKey(found),
            .value = try self.lookupFromEntry(found),
        };
    }

    fn lookupFromEntry(self: Reader, found: Entry) !Lookup {
        if ((found.flags & tombstone_flag) != 0) return .{ .tombstone = .{
            .source_sequence = found.source_sequence,
            .revision = found.revision,
        } };
        const byte_len = std.math.mul(usize, found.dims, self.encoding.componentBytes()) catch return error.CorruptedVectorBlock;
        const bytes = self.data[found.vector_offset..][0..byte_len];
        if (std.hash.Crc32.hash(bytes) != found.vector_checksum) return error.VectorBlockPayloadChecksumMismatch;
        return .{ .vector = .{
            .source_sequence = found.source_sequence,
            .revision = found.revision,
            .dims = found.dims,
            .bytes = bytes,
            .encoding = self.encoding,
            .scale = found.vector_scale,
            .quantization_error_norm = found.quantization_error_norm,
            .decoded_norm_lower_bound = found.decoded_norm_lower_bound,
            .exact_residual = if (found.residual_len == 0)
                null
            else blk: {
                const residual = self.data[found.residual_offset..][0..found.residual_len];
                if (std.hash.Crc32.hash(residual) != found.residual_checksum)
                    return error.VectorBlockResidualChecksumMismatch;
                break :blk residual;
            },
        } };
    }

    fn entry(self: Reader, index: usize) !Entry {
        if (index >= self.count) return error.CorruptedVectorBlock;
        const raw = self.data[self.index_offset + index * self.index_entry_size ..][0..self.index_entry_size];
        const entry_value: Entry = .{
            .hash = readU64(raw[0..8]),
            .key_offset = std.math.cast(usize, readU64(raw[8..16])) orelse return error.CorruptedVectorBlock,
            .key_len = readU32(raw[16..20]),
            .key_checksum = readU32(raw[20..24]),
            .source_sequence = readU64(raw[24..32]),
            .revision = readU64(raw[32..40]),
            .vector_offset = std.math.cast(usize, readU64(raw[40..48])) orelse return error.CorruptedVectorBlock,
            .dims = readU32(raw[48..52]),
            .flags = readU32(raw[52..56]),
            .vector_checksum = readU32(raw[56..60]),
            .vector_scale = if (self.index_entry_size >= scaled_index_entry_size)
                @bitCast(readU32(raw[60..64]))
            else if ((readU32(raw[52..56]) & tombstone_flag) != 0)
                0
            else
                1,
            .quantization_error_norm = if (self.index_entry_size == index_entry_size)
                @bitCast(readU32(raw[64..68]))
            else
                null,
            .decoded_norm_lower_bound = if (self.index_entry_size == index_entry_size)
                @bitCast(readU32(raw[68..72]))
            else
                null,
            .residual_offset = if (self.index_entry_size == index_entry_size)
                std.math.cast(usize, readU64(raw[72..80])) orelse return error.CorruptedVectorBlock
            else
                0,
            .residual_len = if (self.index_entry_size == index_entry_size) readU32(raw[80..84]) else 0,
            .residual_checksum = if (self.index_entry_size == index_entry_size) readU32(raw[84..88]) else 0,
        };
        if (entry_value.key_offset < header_size or entry_value.key_offset > self.index_offset or entry_value.key_len > self.index_offset - entry_value.key_offset) return error.CorruptedVectorBlock;
        if (entry_value.source_sequence > self.covered_source_sequence) return error.CorruptedVectorBlock;
        if ((entry_value.flags & ~tombstone_flag) != 0) return error.CorruptedVectorBlock;
        const tombstone = (entry_value.flags & tombstone_flag) != 0;
        if (tombstone) {
            if (entry_value.vector_offset != 0 or entry_value.dims != 0 or entry_value.vector_checksum != 0 or entry_value.vector_scale != 0 or
                entry_value.quantization_error_norm != null and entry_value.quantization_error_norm.? != 0 or
                entry_value.decoded_norm_lower_bound != null and entry_value.decoded_norm_lower_bound.? != 0 or
                entry_value.residual_offset != 0 or entry_value.residual_len != 0 or entry_value.residual_checksum != 0)
                return error.CorruptedVectorBlock;
        } else {
            if (!std.math.isFinite(entry_value.vector_scale) or entry_value.vector_scale <= 0) return error.CorruptedVectorBlock;
            if (entry_value.quantization_error_norm) |error_norm| {
                const decoded_norm = entry_value.decoded_norm_lower_bound orelse return error.CorruptedVectorBlock;
                validateQuantizationStats(self.encoding, .{
                    .error_norm = error_norm,
                    .decoded_norm_lower_bound = decoded_norm,
                }) catch return error.CorruptedVectorBlock;
            } else if (entry_value.decoded_norm_lower_bound != null) return error.CorruptedVectorBlock;
            if (entry_value.dims == 0 or entry_value.vector_offset < header_size or entry_value.vector_offset % self.encoding.alignment() != 0) return error.CorruptedVectorBlock;
            const byte_len = std.math.mul(usize, entry_value.dims, self.encoding.componentBytes()) catch return error.CorruptedVectorBlock;
            if (entry_value.vector_offset > self.index_offset or byte_len > self.index_offset - entry_value.vector_offset) return error.CorruptedVectorBlock;
            if (entry_value.residual_len == 0) {
                if (entry_value.residual_offset != 0 or entry_value.residual_checksum != 0) return error.CorruptedVectorBlock;
            } else {
                if (self.encoding != .float16 or entry_value.residual_offset < header_size or
                    entry_value.residual_offset > self.index_offset or entry_value.residual_len > self.index_offset - entry_value.residual_offset)
                    return error.CorruptedVectorBlock;
            }
        }
        return entry_value;
    }

    fn entryAssumeValidated(self: Reader, index: usize) Entry {
        std.debug.assert(index < self.count);
        const raw = self.data[self.index_offset + index * self.index_entry_size ..][0..self.index_entry_size];
        return .{
            .hash = readU64(raw[0..8]),
            .key_offset = @intCast(readU64(raw[8..16])),
            .key_len = readU32(raw[16..20]),
            .key_checksum = readU32(raw[20..24]),
            .source_sequence = readU64(raw[24..32]),
            .revision = readU64(raw[32..40]),
            .vector_offset = @intCast(readU64(raw[40..48])),
            .dims = readU32(raw[48..52]),
            .flags = readU32(raw[52..56]),
            .vector_checksum = readU32(raw[56..60]),
            .vector_scale = if (self.index_entry_size >= scaled_index_entry_size)
                @bitCast(readU32(raw[60..64]))
            else if ((readU32(raw[52..56]) & tombstone_flag) != 0)
                0
            else
                1,
            .quantization_error_norm = if (self.index_entry_size == index_entry_size)
                @bitCast(readU32(raw[64..68]))
            else
                null,
            .decoded_norm_lower_bound = if (self.index_entry_size == index_entry_size)
                @bitCast(readU32(raw[68..72]))
            else
                null,
            .residual_offset = if (self.index_entry_size == index_entry_size) @intCast(readU64(raw[72..80])) else 0,
            .residual_len = if (self.index_entry_size == index_entry_size) readU32(raw[80..84]) else 0,
            .residual_checksum = if (self.index_entry_size == index_entry_size) readU32(raw[84..88]) else 0,
        };
    }

    fn entryKey(self: Reader, entry_value: Entry) ![]const u8 {
        const bytes = self.data[entry_value.key_offset..][0..entry_value.key_len];
        if (std.hash.Crc32.hash(bytes) != entry_value.key_checksum) return error.VectorBlockKeyChecksumMismatch;
        return bytes;
    }

    fn compareEntryKeyAssumeValidated(self: Reader, entry_value: Entry, hash: u64, key_value: []const u8) std.math.Order {
        if (entry_value.hash != hash) return std.math.order(entry_value.hash, hash);
        const key = self.data[entry_value.key_offset..][0..entry_value.key_len];
        return std.mem.order(u8, key, key_value);
    }

    fn validate(self: Reader) !void {
        var previous: ?Writer.OrderingKey = null;
        for (0..self.count) |i| {
            const current = try self.entry(i);
            const current_key = try self.entryKey(current);
            if (keyHash(current_key) != current.hash) return error.CorruptedVectorBlock;
            if ((current.hash & (@as(u64, self.shard_count) - 1)) != self.shard_id) return error.CorruptedVectorBlock;
            const ordering: Writer.OrderingKey = .{
                .hash = current.hash,
                .key = current_key,
                .source_sequence = current.source_sequence,
                .revision = current.revision,
            };
            if (previous) |old| if (compareOrdering(old, ordering) != .lt) return error.CorruptedVectorBlock;
            previous = ordering;
        }
    }

    /// True when every live float16 entry can reconstruct its authoritative
    /// float32 source without consulting primary artifact storage.
    pub fn hasExactResiduals(self: Reader) bool {
        if (self.encoding == .float32) return true;
        for (0..self.count) |i| {
            const current = self.entryAssumeValidated(i);
            if ((current.flags & tombstone_flag) == 0 and current.residual_len == 0) return false;
        }
        return true;
    }
};

fn validShardCount(shard_count: u32) bool {
    return shard_count != 0 and std.math.isPowerOfTwo(shard_count);
}

fn compareOrdering(lhs: Writer.OrderingKey, rhs: Writer.OrderingKey) std.math.Order {
    if (lhs.hash != rhs.hash) return std.math.order(lhs.hash, rhs.hash);
    const key_order = std.mem.order(u8, lhs.key, rhs.key);
    if (key_order != .eq) return key_order;
    if (lhs.source_sequence != rhs.source_sequence) return std.math.order(lhs.source_sequence, rhs.source_sequence);
    return std.math.order(lhs.revision, rhs.revision);
}

fn appendU16(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u16) !void {
    var buf: [2]u8 = undefined;
    writeU16(&buf, value);
    try out.appendSlice(alloc, &buf);
}

fn appendU32(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    writeU32(&buf, value);
    try out.appendSlice(alloc, &buf);
}

fn appendU64(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buf: [8]u8 = undefined;
    writeU64(&buf, value);
    try out.appendSlice(alloc, &buf);
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

test "vector block preserves generation revisions and tombstones" {
    const alloc = std.testing.allocator;
    const shard_count: u32 = 1;
    var writer = try Writer.init(alloc, 7, 0, shard_count, 30);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 10, 1, &.{ 1.0, 2.0, 3.0 });
    try writer.appendVector("artifact-a", 20, 2, &.{ 4.0, 5.0, 6.0 });
    try writer.appendTombstone("artifact-a", 30, 3);
    const encoded = try writer.build();
    defer alloc.free(encoded);

    const reader = try Reader.init(encoded);
    try std.testing.expectEqual(@as(u64, 7), reader.generation);
    try std.testing.expectEqual(@as(u64, 30), reader.covered_source_sequence);
    const first_entry = try reader.entry(0);
    const second_entry = try reader.entry(1);
    const tombstone_entry = try reader.entry(2);
    // Admission walks every key. Keep that compact region wholly before the
    // aligned vector arena so validation cannot fault vector payload pages in.
    try std.testing.expect(first_entry.key_offset + first_entry.key_len <= second_entry.key_offset);
    try std.testing.expect(second_entry.key_offset + second_entry.key_len <= tombstone_entry.key_offset);
    try std.testing.expect(tombstone_entry.key_offset + tombstone_entry.key_len <= first_entry.vector_offset);
    try std.testing.expectEqual(first_entry.vector_offset + 3 * @sizeOf(f32), second_entry.vector_offset);
    try std.testing.expect(second_entry.vector_offset + 3 * @sizeOf(f32) <= reader.index_offset);
    const old = try reader.get("artifact-a", 15, 1);
    try std.testing.expectEqual(@as(u64, 10), old.vector.source_sequence);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, old.vector.vectorView().?);
    const old_hashed = try reader.getHashed("artifact-a", keyHash("artifact-a"), 15, 1);
    try std.testing.expectEqual(@as(u64, 10), old_hashed.vector.source_sequence);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0, 3.0 }, old_hashed.vector.vectorView().?);
    try std.testing.expectEqual(@as(u64, 2), (try reader.get("artifact-a", 25, null)).vector.revision);
    try std.testing.expectEqual(@as(u64, 3), (try reader.get("artifact-a", 30, null)).tombstone.revision);
    try std.testing.expectError(error.VectorBlockRevisionMismatch, reader.get("artifact-a", 25, 1));
    try std.testing.expect((try reader.get("missing", 30, null)) == .missing);
}

test "vector block rejects wrapped index regions before slicing" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0, 1, 1);
    defer writer.deinit();
    try writer.appendVector("artifact", 1, 1, &.{1});
    const encoded = try writer.build();
    defer alloc.free(encoded);

    const footer = encoded[encoded.len - footer_size ..];
    writeU64(footer[0..8], std.math.maxInt(u64) - 31);
    writeU64(footer[8..16], 1);
    writeU32(footer[48..52], std.hash.Crc32.hash(footer[0..48]));
    try std.testing.expectError(error.CorruptedVectorBlock, Reader.init(encoded));
}

test "vector block float16 projection round trips through explicit encoding" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 9, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 7, &.{ 0.125, -0.33325, 4.5 });
    const encoded = try writer.build();
    defer alloc.free(encoded);

    const reader = try Reader.init(encoded);
    try std.testing.expectEqual(Encoding.float16, reader.encoding);
    const value = (try reader.get("artifact-a", 1, 7)).vector;
    try std.testing.expect(value.vectorView() == null);
    try std.testing.expectEqual(@as(usize, 3 * @sizeOf(f16)), value.bytes.len);
    var decoded: [3]f32 = undefined;
    const view = try value.decodeInto(&decoded);
    try std.testing.expectApproxEqAbs(@as(f32, 0.125), view[0], 0.0005);
    try std.testing.expectApproxEqAbs(@as(f32, -0.33325), view[1], 0.0005);
    try std.testing.expectApproxEqAbs(@as(f32, 4.5), view[2], 0.0005);
    const error_norm = value.quantization_error_norm orelse return error.TestExpectedQuantizationMetadata;
    const decoded_norm_lower = value.decoded_norm_lower_bound orelse return error.TestExpectedQuantizationMetadata;
    var actual_error_squared: f64 = 0;
    var actual_decoded_norm_squared: f64 = 0;
    for (view, [_]f32{ 0.125, -0.33325, 4.5 }) |decoded_value, source_value| {
        const component_error = source_value - decoded_value;
        actual_error_squared += @as(f64, component_error) * component_error;
        actual_decoded_norm_squared += @as(f64, decoded_value) * decoded_value;
    }
    try std.testing.expect(@sqrt(actual_error_squared) <= error_norm);
    try std.testing.expect(decoded_norm_lower <= @sqrt(actual_decoded_norm_squared));
    try std.testing.expect(reader.hasExactResiduals());
    var exact: [3]f32 = undefined;
    const exact_view = try value.decodeExactInto(&exact);
    for (exact_view, [_]f32{ 0.125, -0.33325, 4.5 }) |found, expected| {
        try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(found)));
    }
}

test "vector block residual reconstructs adversarial float32 bits exactly" {
    const alloc = std.testing.allocator;
    const source = [_]f32{
        0.0,
        @bitCast(@as(u32, 0x80000000)),
        @bitCast(@as(u32, 1)),
        @bitCast(@as(u32, 0x80000001)),
        0.33333334,
        -12_345.678,
        100_000.0,
        -250_000.0,
        std.math.floatMax(f32),
    };
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 1, &source);
    const block = try writer.build();
    defer alloc.free(block);

    const reader = try Reader.init(block);
    try std.testing.expect(reader.hasExactResiduals());
    const value = (try reader.get("artifact-a", 1, 1)).vector;
    try std.testing.expect(value.exact_residual != null);
    var exact: [source.len]f32 = undefined;
    const decoded = try value.decodeExactInto(&exact);
    for (source, decoded) |expected, found| {
        try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(found)));
    }
}

test "vector block keeps pre-encoded float16 bounded until residual is supplied" {
    const alloc = std.testing.allocator;
    const source = [_]f32{ 0.1, -0.2, 0.3 };
    var payload: [source.len * @sizeOf(f16)]u8 = undefined;
    const encoded = try encodeVectorIntoWithStats(.float16, &source, &payload);
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendEncodedVectorWithStats("artifact-a", 1, 1, source.len, &payload, encoded.scale, encoded.quantization);
    const block = try writer.build();
    defer alloc.free(block);
    const reader = try Reader.init(block);
    try std.testing.expect(!reader.hasExactResiduals());
    var scratch: [source.len]f32 = undefined;
    try std.testing.expectError(
        error.ExactVectorResidualMissing,
        (try reader.get("artifact-a", 1, 1)).vector.decodeExactInto(&scratch),
    );
}

test "vector block reader keeps v3 scaled blocks compatible without projection metadata" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 9, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 7, &.{ 0.125, -0.33325, 4.5 });
    const current = try writer.build();
    defer alloc.free(current);
    const current_reader = try Reader.init(current);

    // A v3 entry ends after vector_scale. Reframe the committed current block
    // as the exact bytes an older writer emitted, including both checksums.
    const old = try alloc.alloc(u8, current.len - (index_entry_size - scaled_index_entry_size));
    defer alloc.free(old);
    const old_footer_start = old.len - footer_size;
    @memcpy(old[0 .. current_reader.index_offset + scaled_index_entry_size], current[0 .. current_reader.index_offset + scaled_index_entry_size]);
    @memcpy(old[old_footer_start..], current[current.len - footer_size ..]);
    writeU16(old[8..10], scaled_encoding_version);
    writeU32(old[36..40], std.hash.Crc32.hash(old[0..36]));
    const footer = old[old_footer_start..];
    writeU32(footer[32..36], std.hash.Crc32.hash(old[current_reader.index_offset..old_footer_start]));
    writeU16(footer[36..38], scaled_encoding_version);
    writeU32(footer[48..52], std.hash.Crc32.hash(footer[0..48]));

    const reader = try Reader.init(old);
    const value = (try reader.get("artifact-a", 1, 7)).vector;
    try std.testing.expect(value.quantization_error_norm == null);
    try std.testing.expect(value.decoded_norm_lower_bound == null);
    var decoded: [3]f32 = undefined;
    try std.testing.expectApproxEqAbs(@as(f32, -0.33325), (try value.decodeInto(&decoded))[1], 0.0005);
}

test "vector block admission rejects invalid projection metadata" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 1, &.{ 1.0, 2.0 });
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const reader = try Reader.init(encoded);
    writeU32(encoded[reader.index_offset + 64 ..][0..4], @bitCast(std.math.nan(f32)));
    const footer = encoded[encoded.len - footer_size ..];
    writeU32(footer[32..36], std.hash.Crc32.hash(encoded[reader.index_offset .. encoded.len - footer_size]));
    writeU32(footer[48..52], std.hash.Crc32.hash(footer[0..48]));
    try std.testing.expectError(error.CorruptedVectorBlock, Reader.init(encoded));
}

test "vector block float16 projection scales values outside its native finite domain" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 1, &.{ 100_000.0, -250_000.0 });
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const reader = try Reader.init(encoded);
    const value = (try reader.get("artifact-a", 1, 1)).vector;
    try std.testing.expect(value.scale > 1);
    try std.testing.expect(value.quantization_error_norm != null);
    try std.testing.expect(value.decoded_norm_lower_bound != null);
    var decoded: [2]f32 = undefined;
    const view = try value.decodeInto(&decoded);
    try std.testing.expectApproxEqRel(@as(f32, 100_000.0), view[0], 0.001);
    try std.testing.expectApproxEqRel(@as(f32, -250_000.0), view[1], 0.001);
}

test "vector block accepts validated pre-encoded payload without changing representation" {
    const alloc = std.testing.allocator;
    const source = [_]f32{ 100_000.0, -250_000.0, 6.0 };
    var payload: [source.len * @sizeOf(f16)]u8 = undefined;
    const scale = try encodeVectorInto(.float16, &source, &payload);

    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendEncodedVector("artifact-a", 1, 1, source.len, &payload, scale);
    const block = try writer.build();
    defer alloc.free(block);

    const value = (try (try Reader.init(block)).get("artifact-a", 1, 1)).vector;
    try std.testing.expectEqual(scale, value.scale);
    try std.testing.expectEqualSlices(u8, &payload, value.bytes);
    try std.testing.expect(value.quantization_error_norm != null);
    try std.testing.expect(value.decoded_norm_lower_bound != null);
    var decoded: [source.len]f32 = undefined;
    const actual = try value.decodeInto(&decoded);
    for (source, actual) |expected, found| try std.testing.expectApproxEqRel(expected, found, 0.001);
}

test "vector block rejects malformed pre-encoded payload before writer mutation" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try std.testing.expectError(
        error.InvalidEncodedVectorLength,
        writer.appendEncodedVector("artifact-a", 1, 1, 2, &.{0}, 1),
    );
    var non_finite: [2]u8 = undefined;
    std.mem.writeInt(u16, &non_finite, @bitCast(std.math.inf(f16)), .little);
    try std.testing.expectError(
        error.InvalidVectorComponent,
        writer.appendEncodedVector("artifact-a", 1, 1, 1, &non_finite, 1),
    );
    try writer.appendVector("artifact-a", 1, 1, &.{1.0});
    const block = try writer.build();
    defer alloc.free(block);
    _ = try Reader.init(block);
}

test "vector block rejects non-finite components before mutating writer state" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try std.testing.expectError(error.InvalidVectorComponent, writer.appendVector("artifact-a", 1, 1, &.{std.math.nan(f32)}));
    try writer.appendVector("artifact-a", 1, 1, &.{1.0});
    const encoded = try writer.build();
    defer alloc.free(encoded);
    _ = try Reader.init(encoded);
}

test "vector block validates shards and entry ordering" {
    const alloc = std.testing.allocator;
    const key = "artifact-for-shard";
    const shard_count: u32 = 8;
    const shard_id = try shardForKey(key, shard_count);
    var writer = try Writer.init(alloc, 1, shard_id, shard_count, 5);
    defer writer.deinit();
    try writer.appendVector(key, 5, 1, &.{ 0.25, -0.5 });
    try std.testing.expectError(error.OutOfOrderVectorBlockEntry, writer.appendVector(key, 5, 1, &.{1.0}));
    try std.testing.expectError(error.VectorBlockSequenceBeyondCoverage, writer.appendVector(key, 6, 2, &.{1.0}));

    const wrong_shard = (shard_id + 1) % shard_count;
    var wrong = try Writer.init(alloc, 1, wrong_shard, shard_count, 5);
    defer wrong.deinit();
    try std.testing.expectError(error.WrongVectorBlockShard, wrong.appendVector(key, 5, 1, &.{1.0}));
}

test "vector block detects lazy vector corruption" {
    const alloc = std.testing.allocator;
    var writer = try Writer.init(alloc, 1, 0, 1, 1);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 1, &.{ 1.0, 2.0 });
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const pristine = try Reader.init(encoded);
    const value = (try pristine.get("artifact-a", 1, null)).vector;
    encoded[@intFromPtr(value.bytes.ptr) - @intFromPtr(encoded.ptr)] ^= 0xff;
    const reader = try Reader.init(encoded);
    try std.testing.expectError(error.VectorBlockPayloadChecksumMismatch, reader.get("artifact-a", 1, null));
}

test "vector block detects residual corruption lazily" {
    const alloc = std.testing.allocator;
    var writer = try Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 1, 1, &.{ 0.1, -0.2, 0.3 });
    const encoded = try writer.build();
    defer alloc.free(encoded);
    const pristine = try Reader.init(encoded);
    const residual = (try pristine.get("artifact-a", 1, null)).vector.exact_residual.?;
    encoded[@intFromPtr(residual.ptr) - @intFromPtr(encoded.ptr)] ^= 0xff;
    const reader = try Reader.init(encoded);
    try std.testing.expectError(error.VectorBlockResidualChecksumMismatch, reader.get("artifact-a", 1, null));
}
