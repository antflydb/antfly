// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

pub const magic = "ABLOOM1";
pub const version: u32 = 1;

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
        if (bit_count == 0 and byte_len != 0) return error.InvalidBloomFilter;
        if (bit_count != 0 and byte_len != ((bit_count + 7) / 8)) return error.InvalidBloomFilter;
        return .{
            .bytes = bytes,
            .bit_count = bit_count,
            .hash_count = hash_count,
        };
    }
};

fn maybeContainsHashesImpl(bytes: []const u8, bit_count_value: u32, hash_count_value: u8, h1: u64, h2_in: u64) bool {
    if (bit_count_value == 0 or hash_count_value == 0) return false;
    const h2 = if (h2_in == 0) 0x9e3779b97f4a7c15 else h2_in;
    const bit_count: u64 = bit_count_value;
    var i: u8 = 0;
    while (i < hash_count_value) : (i += 1) {
        const bit_index: usize = @intCast((h1 +% (@as(u64, i) *% h2)) % bit_count);
        const byte_index = bit_index / 8;
        const bit_mask: u8 = @as(u8, 1) << @intCast(bit_index % 8);
        if ((bytes[byte_index] & bit_mask) == 0) return false;
    }
    return true;
}

pub const Builder = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    bit_count: u32,
    hash_count: u8,

    pub fn init(allocator: std.mem.Allocator, key_count: usize, config: Config) !Builder {
        const raw_bits = @max(config.min_bits, key_count * config.bits_per_key);
        const bit_count = if (key_count == 0) 0 else std.math.ceilPowerOfTwo(usize, raw_bits) catch raw_bits;
        const byte_len = if (bit_count == 0) 0 else (bit_count + 7) / 8;
        const bytes = try allocator.alloc(u8, byte_len);
        @memset(bytes, 0);
        return .{
            .allocator = allocator,
            .bytes = bytes,
            .bit_count = @intCast(bit_count),
            .hash_count = optimalHashCount(config.bits_per_key, config.max_hash_count),
        };
    }

    pub fn deinit(self: *Builder) void {
        self.allocator.free(self.bytes);
        self.* = undefined;
    }

    pub fn addHashes(self: *Builder, h1: u64, h2_in: u64) void {
        if (self.bit_count == 0 or self.hash_count == 0) return;
        const h2 = if (h2_in == 0) 0x9e3779b97f4a7c15 else h2_in;
        const bit_count: u64 = self.bit_count;
        var i: u8 = 0;
        while (i < self.hash_count) : (i += 1) {
            const bit_index: usize = @intCast((h1 +% (@as(u64, i) *% h2)) % bit_count);
            const byte_index = bit_index / 8;
            const bit_mask: u8 = @as(u8, 1) << @intCast(bit_index % 8);
            self.bytes[byte_index] |= bit_mask;
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
