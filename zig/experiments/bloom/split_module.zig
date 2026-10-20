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

// EXPERIMENT ONLY. Drop-in storage-benchmark module; not a production format.
// Its distinct magic/version prevent accidental reads by existing readers.
const std = @import("std");
const split_salts = [_]u32{ 0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31 };

pub const magic = "ABLOOM2";
pub const version: u32 = 2;

pub const Config = struct {
    bits_per_key: usize = 10,
    min_bits: usize = 64,
    max_hash_count: u8 = 16,
};

pub const OwnedFilter = struct {
    bytes: []u8,
    bit_count: u32,
    hash_count: u8,

    pub fn deinit(self: *OwnedFilter, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
        self.* = undefined;
    }

    pub fn clone(self: OwnedFilter, allocator: std.mem.Allocator) !OwnedFilter {
        return .{
            .bytes = try allocator.dupe(u8, self.bytes),
            .bit_count = self.bit_count,
            .hash_count = self.hash_count,
        };
    }

    pub fn maybeContainsHashes(self: OwnedFilter, h1: u64, h2_in: u64) bool {
        return maybeContainsHashesImpl(self.bytes, self.bit_count, self.hash_count, h1, h2_in);
    }

    pub fn encodeAlloc(self: OwnedFilter, allocator: std.mem.Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(allocator);
        _ = try self.encodeInto(allocator, &out);
        return try out.toOwnedSlice(allocator);
    }

    pub fn encodeInto(self: OwnedFilter, allocator: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8)) ![]const u8 {
        out.clearRetainingCapacity();
        try out.ensureTotalCapacity(allocator, encodedLen(self));
        try out.appendSlice(allocator, magic);
        try appendU32(allocator, out, version);
        try appendU32(allocator, out, self.bit_count);
        try out.append(allocator, self.hash_count);
        try appendU32(allocator, out, @intCast(self.bytes.len));
        try out.appendSlice(allocator, self.bytes);
        return out.items;
    }

    pub fn decodeAlloc(allocator: std.mem.Allocator, raw: []const u8) !OwnedFilter {
        const borrowed = try BorrowedFilter.decode(raw);
        return borrowed.clone(allocator);
    }
};

pub const BorrowedFilter = struct {
    bytes: []const u8,
    bit_count: u32,
    hash_count: u8,

    pub fn maybeContainsHashes(self: BorrowedFilter, h1: u64, h2_in: u64) bool {
        return maybeContainsHashesImpl(self.bytes, self.bit_count, self.hash_count, h1, h2_in);
    }

    pub fn clone(self: BorrowedFilter, allocator: std.mem.Allocator) !OwnedFilter {
        return .{
            .bytes = try allocator.dupe(u8, self.bytes),
            .bit_count = self.bit_count,
            .hash_count = self.hash_count,
        };
    }

    pub fn decode(raw: []const u8) !BorrowedFilter {
        var cursor: usize = 0;
        if (raw.len < magic.len + 13) return error.InvalidBloomFilter;
        if (!std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidBloomFilter;
        cursor += magic.len;

        const found_version = try readU32(raw, &cursor);
        if (found_version != version) return error.UnsupportedVersion;

        const bit_count = try readU32(raw, &cursor);
        const hash_count = try readByte(raw, &cursor);
        const byte_len: usize = @intCast(try readU32(raw, &cursor));
        const bytes = try readSlice(raw, &cursor, byte_len);

        if (cursor != raw.len) return error.InvalidBloomFilter;
        if (bit_count != 0 and (bit_count % 256 != 0 or (hash_count != 0 and hash_count != 8))) return error.InvalidBloomFilter;
        if (bit_count == 0 and byte_len != 0) return error.InvalidBloomFilter;
        if (bit_count != 0 and byte_len != ((bit_count + 7) / 8)) return error.InvalidBloomFilter;
        return .{
            .bytes = bytes,
            .bit_count = bit_count,
            .hash_count = hash_count,
        };
    }
};

fn maybeContainsHashesImpl(bytes: []const u8, bit_count_value: u32, hash_count_value: u8, h1: u64, h2: u64) bool {
    if (bit_count_value == 0 or hash_count_value == 0) return false;
    const block: usize = @intCast((@as(u128, h1) * (bit_count_value / 256)) >> 64);
    const offset = block * 32;
    const h: u32 = @truncate(h2);
    var missing: u32 = 0;
    inline for (split_salts, 0..) |salt, i| {
        const mask = @as(u32, 1) << @intCast((h *% salt) >> 27);
        const word = std.mem.readInt(u32, bytes[offset + i * 4 ..][0..4], .little);
        missing |= (word & mask) ^ mask;
    }
    return missing == 0;
}

pub const Builder = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    bit_count: u32,
    hash_count: u8,

    pub fn init(allocator: std.mem.Allocator, key_count: usize, config: Config) !Builder {
        const raw_bits = @max(config.min_bits, key_count * config.bits_per_key);
        const original_bits = std.math.ceilPowerOfTwo(usize, raw_bits) catch raw_bits;
        const bit_count = if (key_count == 0) 0 else std.mem.alignForward(usize, original_bits * 130 / 100, 256);
        const byte_len = if (bit_count == 0) 0 else (bit_count + 7) / 8;
        const bytes = try allocator.alloc(u8, byte_len);
        @memset(bytes, 0);
        return .{
            .allocator = allocator,
            .bytes = bytes,
            .bit_count = @intCast(bit_count),
            .hash_count = if (config.bits_per_key == 0 or config.max_hash_count == 0) 0 else 8,
        };
    }

    pub fn deinit(self: *Builder) void {
        self.allocator.free(self.bytes);
        self.* = undefined;
    }

    pub fn addHashes(self: *Builder, h1: u64, h2: u64) void {
        if (self.bit_count == 0 or self.hash_count == 0) return;
        const block: usize = @intCast((@as(u128, h1) * (self.bit_count / 256)) >> 64);
        const offset = block * 32;
        const h: u32 = @truncate(h2);
        inline for (split_salts, 0..) |salt, i| {
            const mask = @as(u32, 1) << @intCast((h *% salt) >> 27);
            const lane = self.bytes[offset + i * 4 ..][0..4];
            const word = std.mem.readInt(u32, lane, .little);
            std.mem.writeInt(u32, lane, word | mask, .little);
        }
    }

    pub fn finish(self: *Builder) OwnedFilter {
        const filter = OwnedFilter{
            .bytes = self.bytes,
            .bit_count = self.bit_count,
            .hash_count = self.hash_count,
        };
        self.* = undefined;
        return filter;
    }
};

pub fn hashBytes(seed: u64, bytes: []const u8) u64 {
    return std.hash.Wyhash.hash(seed, bytes);
}

pub fn optimalHashCount(bits_per_key: usize, max_hash_count: u8) u8 {
    if (bits_per_key == 0) return 0;
    const estimate = @max(1, (bits_per_key * 69 + 50) / 100);
    return @intCast(@min(estimate, max_hash_count));
}

fn appendU32(allocator: std.mem.Allocator, bytes: *std.ArrayListUnmanaged(u8), value: u32) !void {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, value, .little);
    try bytes.appendSlice(allocator, &buf);
}

fn encodedLen(self: OwnedFilter) usize {
    return magic.len + 4 + 4 + 1 + 4 + self.bytes.len;
}

fn readByte(raw: []const u8, cursor: *usize) !u8 {
    if (cursor.* >= raw.len) return error.InvalidBloomFilter;
    const out = raw[cursor.*];
    cursor.* += 1;
    return out;
}

fn readU32(raw: []const u8, cursor: *usize) !u32 {
    const bytes = try readSlice(raw, cursor, 4);
    return std.mem.readInt(u32, bytes[0..4], .little);
}

fn readSlice(raw: []const u8, cursor: *usize, len: usize) ![]const u8 {
    if (cursor.* + len > raw.len) return error.InvalidBloomFilter;
    const out = raw[cursor.* .. cursor.* + len];
    cursor.* += len;
    return out;
}

test "bloom filter round trips and preserves membership for inserted hashes" {
    var builder = try Builder.init(std.testing.allocator, 3, .{});
    errdefer builder.deinit();

    const hashes = [_][2]u64{
        .{ hashBytes(0, "alpha"), hashBytes(1, "alpha") },
        .{ hashBytes(0, "beta"), hashBytes(1, "beta") },
        .{ hashBytes(0, "gamma"), hashBytes(1, "gamma") },
    };
    for (hashes) |pair| builder.addHashes(pair[0], pair[1]);

    var filter = builder.finish();
    defer filter.deinit(std.testing.allocator);

    for (hashes) |pair| {
        try std.testing.expect(filter.maybeContainsHashes(pair[0], pair[1]));
    }

    const encoded = try filter.encodeAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);

    var decoded = try OwnedFilter.decodeAlloc(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);

    for (hashes) |pair| {
        try std.testing.expect(decoded.maybeContainsHashes(pair[0], pair[1]));
    }
}

test "bloom encodeInto reuses output buffer" {
    const alloc = std.testing.allocator;
    var builder = try Builder.init(alloc, 2, .{});
    errdefer builder.deinit();
    builder.addHashes(hashBytes(0, "alpha"), hashBytes(1, "alpha"));
    builder.addHashes(hashBytes(0, "beta"), hashBytes(1, "beta"));

    var filter = builder.finish();
    defer filter.deinit(alloc);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    const first = try filter.encodeInto(alloc, &out);
    try std.testing.expect(first.len > 0);
    const capacity_after_first = out.capacity;

    const second = try filter.encodeInto(alloc, &out);
    try std.testing.expect(second.len == first.len);
    try std.testing.expect(out.capacity == capacity_after_first);

    var decoded = try OwnedFilter.decodeAlloc(alloc, second);
    defer decoded.deinit(alloc);
    try std.testing.expect(decoded.maybeContainsHashes(hashBytes(0, "alpha"), hashBytes(1, "alpha")));
    try std.testing.expect(decoded.maybeContainsHashes(hashBytes(0, "beta"), hashBytes(1, "beta")));
}

test "experimental split roundtrip preserves all keys and rejects legacy magic" {
    const allocator = std.testing.allocator;
    var builder = try Builder.init(allocator, 100000, .{ .bits_per_key = 14 });
    errdefer builder.deinit();
    for (0..100000) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .little);
        builder.addHashes(hashBytes(0, &key), hashBytes(1, &key));
    }
    var filter = builder.finish();
    defer filter.deinit(allocator);
    const encoded = try filter.encodeAlloc(allocator);
    defer allocator.free(encoded);
    // Exercise an unaligned, borrowed serialized payload.
    const unaligned = try allocator.alloc(u8, encoded.len + 1);
    defer allocator.free(unaligned);
    @memcpy(unaligned[1..], encoded);
    const decoded = try BorrowedFilter.decode(unaligned[1..]);
    var fp: usize = 0;
    for (0..200000) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .little);
        const found = decoded.maybeContainsHashes(hashBytes(0, &key), hashBytes(1, &key));
        if (i < 100000) try std.testing.expect(found) else fp += @intFromBool(found);
    }
    try std.testing.expect(fp < 1000);
    @memcpy(encoded[0..magic.len], "ABLOOM1");
    try std.testing.expectError(error.InvalidBloomFilter, BorrowedFilter.decode(encoded));
}

test "experimental split empty filter stays empty after encoding" {
    var builder = try Builder.init(std.testing.allocator, 0, .{});
    var filter = builder.finish();
    defer filter.deinit(std.testing.allocator);
    try std.testing.expect(!filter.maybeContainsHashes(0, 0));
    const encoded = try filter.encodeAlloc(std.testing.allocator);
    defer std.testing.allocator.free(encoded);
    const decoded = try BorrowedFilter.decode(encoded);
    try std.testing.expect(!decoded.maybeContainsHashes(1, 2));
}
