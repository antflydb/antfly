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

//! Canonical, location-independent Merkle input commitments. Fixed radix
//! buckets localize a change without algebraically composing row hashes.
const std = @import("std");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const A = std.mem.Allocator;
const Hash = std.crypto.hash.Blake3;
pub const bucket_count = 256;
pub const State = struct {
    counts: [bucket_count]u32 = @splat(0),
    digests: [bucket_count][32]u8 = undefined,
    pub const bytes = bucket_count * 36;
    pub fn decode(encoded: []const u8) !State {
        if (encoded.len != bytes) return error.InvalidLogicalRowIdentity;
        var result: State = undefined;
        for (&result.counts, &result.digests, 0..) |*count, *digest, i| {
            count.* = std.mem.readInt(u32, encoded[i * 36 ..][0..4], .big);
            digest.* = encoded[i * 36 + 4 ..][0..32].*;
        }
        return result;
    }
    pub fn encode(self: *const State, output: *[bytes]u8) void {
        for (self.counts, self.digests, 0..) |count, digest, i| {
            std.mem.writeInt(u32, output[i * 36 ..][0..4], count, .big);
            output[i * 36 + 4 ..][0..32].* = digest;
        }
    }
    pub fn root(self: *const State, namespace: [32]u8, cohort: [32]u8, prefix: [32]u8, bits: u16, expected: u64) ![32]u8 {
        var nodes = self.digests;
        var count: u64 = 0;
        for (self.counts) |n| count += n;
        if (count != expected) return error.InvalidLogicalRowIdentity;
        var width: usize = bucket_count;
        while (width > 1) : (width /= 2) for (0..width / 2) |i| {
            var hash = Hash.init(.{});
            hash.update("native-lake-input-merkle-node-v1");
            hash.update(&nodes[i * 2]);
            hash.update(&nodes[i * 2 + 1]);
            hash.final(&nodes[i]);
        };
        var hash = Hash.init(.{});
        hash.update("native-lake-input-merkle-fragment-v1");
        hash.update(&namespace);
        hash.update(&cohort);
        hash.update(&prefix);
        var shape: [10]u8 = undefined;
        std.mem.writeInt(u16, shape[0..2], bits, .big);
        std.mem.writeInt(u64, shape[2..10], count, .big);
        hash.update(&shape);
        hash.update(&nodes[0]);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }
};
pub fn key(cohort: [32]u8, prefix: [32]u8, bits: u16) [66]u8 {
    var result: [66]u8 = undefined;
    result[0..32].* = cohort;
    std.mem.writeInt(u16, result[32..34], bits, .big);
    result[34..66].* = prefix;
    return result;
}
fn upperBound(prefix: [32]u8, bits: u16) ?[32]u8 {
    var result = prefix;
    var bit = bits;
    while (bit != 0) {
        bit -= 1;
        const mask = @as(u8, 128) >> @as(u3, @intCast(bit % 8));
        if (result[bit / 8] & mask == 0) {
            result[bit / 8] |= mask;
            return result;
        }
        result[bit / 8] &= ~mask;
    }
    return null;
}
pub fn bucket(id: [32]u8, bits: u16) usize {
    var result: usize = 0;
    for (bits..@min(256, bits + 8)) |bit| result = (result << 1) | @as(usize, @intFromBool(id[bit / 8] & (@as(u8, 128) >> @as(u3, @intCast(bit % 8))) != 0));
    return result;
}
fn leaf() Hash {
    var hash = Hash.init(.{});
    hash.update("native-lake-input-merkle-leaf-v1");
    return hash;
}
fn add(hash: *Hash, record: tree.Cursor.Record) !void {
    if (record.key.len != 64 or record.value.len != 32) return error.InvalidLogicalRowIdentity;
    hash.update(record.key[32..64]);
    hash.update(record.value);
}
/// A full build reads one ordered range. A delta rehashes only reached radix
/// buckets; the COW commitment record and eight Merkle levels bound metadata.
pub fn build(a: A, storage: tree.Store, inputs: ?tree.Ref, cohort: [32]u8, prefix: [32]u8, bits: u16, prior: ?State, changed: ?[]const [32]u8, hashed: *u64) !State {
    var result: State = prior orelse .{};
    var dirty: [bucket_count]bool = @splat(prior == null or changed == null);
    if (changed) |ids| for (ids) |id| {
        if (std.mem.order(u8, &id, &prefix) == .lt) continue;
        if (upperBound(prefix, bits)) |bound| if (std.mem.order(u8, &id, &bound) != .lt) continue;
        dirty[bucket(id, bits)] = true;
    };
    if (prior == null) {
        var empty = leaf();
        var empty_digest: [32]u8 = undefined;
        empty.final(&empty_digest);
        @memset(&result.digests, empty_digest);
        var hash = leaf();
        var current: ?usize = null;
        const lower = cohort ++ prefix;
        const bound = upperBound(prefix, bits);
        const upper = cohort ++ (bound orelse @as([32]u8, @splat(255)));
        var cursor = try tree.Cursor.init(a, storage, inputs, &lower, if (bound != null) &upper else null);
        defer cursor.deinit();
        while (try cursor.next()) |row| {
            if (row.key.len != 64 or !std.mem.eql(u8, row.key[0..32], &cohort)) break;
            const i = bucket(row.key[32..64].*, bits);
            if (current != null and current.? != i) {
                hash.final(&result.digests[current.?]);
                hash = leaf();
            }
            current = i;
            try add(&hash, row);
            result.counts[i] += 1;
            hashed.* += 1;
        }
        if (current) |i| hash.final(&result.digests[i]);
        return result;
    }
    for (dirty, 0..) |needed, i| if (needed) {
        const width = @min(8, 256 - bits);
        if (i >= @as(usize, 1) << @intCast(width)) continue;
        var lower_id = prefix;
        for (0..width) |bit| if (i & (@as(usize, 1) << @intCast(width - 1 - bit)) != 0) {
            const at = bits + bit;
            lower_id[at / 8] |= @as(u8, 128) >> @as(u3, @intCast(at % 8));
        };
        const lower = cohort ++ lower_id;
        const bound = upperBound(lower_id, bits + width);
        const upper = cohort ++ (bound orelse @as([32]u8, @splat(255)));
        var hash = leaf();
        result.counts[i] = 0;
        var cursor = try tree.Cursor.init(a, storage, inputs, &lower, if (bound != null) &upper else null);
        defer cursor.deinit();
        while (try cursor.next()) |row| {
            if (row.key.len != 64 or !std.mem.eql(u8, row.key[0..32], &cohort)) break;
            try add(&hash, row);
            result.counts[i] += 1;
            hashed.* += 1;
        }
        hash.final(&result.digests[i]);
    };
    return result;
}
