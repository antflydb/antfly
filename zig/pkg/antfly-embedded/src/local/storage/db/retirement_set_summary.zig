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

//! Transactional authenticated retirement set. Compressed binary Patricia
//! branches exist only at actual key divergences: N leaves need N-1 branches,
//! not 128 empty ancestors per leaf. All writes use the caller's transaction.
const std = @import("std");
pub const root_key = "\x00\x00__metadata__:retirement_set_root:v1";
const node_prefix = "\x00\x00__metadata__:retirement_set_node:v1:";
const ref_len = 57;
/// Conservative transaction/WAL reservation, including per-record framing,
/// for the maximum compressed path. Actual nodes coalesce within a batch.
pub const max_mutation_bytes = 130 * (node_prefix.len + 17 + 2 * ref_len + 32) + root_key.len + 61;
const Digest = [32]u8;
const Generation = [16]u8;

pub const Summary = struct { count: u64, digest: Digest };

pub fn isKey(name: []const u8) bool {
    return std.mem.eql(u8, name, root_key) or std.mem.startsWith(u8, name, node_prefix);
}
const Ref = struct {
    depth: u8,
    prefix: Generation,
    count: u64,
    digest: Digest,

    fn encode(self: Ref) [ref_len]u8 {
        var result: [ref_len]u8 = undefined;
        result[0] = self.depth;
        @memcpy(result[1..17], &self.prefix);
        std.mem.writeInt(u64, result[17..25], self.count, .little);
        @memcpy(result[25..57], &self.digest);
        return result;
    }
    fn decode(bytes: []const u8) !Ref {
        if (bytes.len != ref_len or bytes[0] > 128) return error.InvalidRetirementSummary;
        const result: Ref = .{ .depth = bytes[0], .prefix = bytes[1..17].*, .count = std.mem.readInt(u64, bytes[17..25], .little), .digest = bytes[25..57].* };
        if (!std.mem.eql(u8, &result.prefix, &masked(result.prefix, result.depth)) or result.count == 0 or
            (result.depth == 128 and result.count != 1) or
            (result.depth < 128 and result.count < 2)) return error.InvalidRetirementSummary;
        if (result.depth > 64 and result.count > (@as(u64, 1) << @intCast(128 - result.depth)))
            return error.InvalidRetirementSummary;
        return result;
    }
};
const Branch = struct { left: Ref, right: Ref };

fn masked(generation: Generation, depth: u8) Generation {
    var result = generation;
    const complete: usize = depth / 8;
    const remainder = depth % 8;
    if (remainder == 0) {
        @memset(result[complete..], 0);
    } else {
        result[complete] &= @as(u8, 0xff) << @intCast(8 - remainder);
        @memset(result[complete + 1 ..], 0);
    }
    return result;
}
fn bitAt(generation: Generation, depth: u8) bool {
    std.debug.assert(depth < 128);
    return generation[depth / 8] & (@as(u8, 0x80) >> @intCast(depth % 8)) != 0;
}
fn common(a: Generation, b: Generation) u8 {
    for (a, b, 0..) |left, right, i| {
        if (left != right) return @intCast(i * 8 + @clz(left ^ right));
    }
    return 128;
}
fn key(ref: Ref) [node_prefix.len + 17]u8 {
    var result: [node_prefix.len + 17]u8 = undefined;
    @memcpy(result[0..node_prefix.len], node_prefix);
    result[node_prefix.len] = ref.depth;
    @memcpy(result[node_prefix.len + 1 ..], &ref.prefix);
    return result;
}
fn valueDigest(value: []const u8) Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("retirement-set-value-v1");
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, value.len, .little);
    hash.update(&size);
    hash.update(value);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}
fn leaf(generation: Generation, value_hash: Digest) Ref {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("retirement-set-leaf-v1");
    hash.update(&generation);
    hash.update(&value_hash);
    var result: Ref = .{ .depth = 128, .prefix = generation, .count = 1, .digest = undefined };
    hash.final(&result.digest);
    return result;
}
fn branch(left: Ref, right: Ref) !Ref {
    const depth = common(left.prefix, right.prefix);
    if (depth >= left.depth or depth >= right.depth or bitAt(left.prefix, depth) or !bitAt(right.prefix, depth)) return error.InvalidRetirementSummary;
    var result: Ref = .{ .depth = depth, .prefix = masked(left.prefix, depth), .count = std.math.add(u64, left.count, right.count) catch return error.InvalidRetirementSummary, .digest = undefined };
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("retirement-set-branch-v1");
    hash.update(&left.encode());
    hash.update(&right.encode());
    hash.final(&result.digest);
    return result;
}
fn root(txn: anytype) !?Ref {
    const bytes = txn.get(root_key) catch |err| return if (err == error.NotFound) null else err;
    return decodeRoot(bytes);
}
fn decodeRoot(bytes: []const u8) !?Ref {
    if (std.mem.eql(u8, bytes, "RSE1")) return null;
    if (bytes.len != 4 + ref_len or !std.mem.eql(u8, bytes[0..4], "RST1")) return error.InvalidRetirementSummary;
    return try Ref.decode(bytes[4..]);
}
fn storeRoot(txn: anytype, ref: ?Ref) !void {
    if (ref) |present| {
        var bytes: [4 + ref_len]u8 = undefined;
        @memcpy(bytes[0..4], "RST1");
        @memcpy(bytes[4..], &present.encode());
        try txn.put(root_key, &bytes);
    } else try txn.put(root_key, "RSE1");
}
fn loadBranch(txn: anytype, ref: Ref) !Branch {
    const bytes = txn.get(&key(ref)) catch |err| return if (err == error.NotFound) error.InvalidRetirementSummary else err;
    if (ref.depth == 128 or bytes.len != 2 * ref_len) return error.InvalidRetirementSummary;
    const result: Branch = .{ .left = try Ref.decode(bytes[0..ref_len]), .right = try Ref.decode(bytes[ref_len..]) };
    const actual = try branch(result.left, result.right);
    if (!std.meta.eql(actual, ref)) return error.InvalidRetirementSummary;
    return result;
}
fn storeBranch(txn: anytype, children: Branch) !Ref {
    const ref = try branch(children.left, children.right);
    var bytes: [2 * ref_len]u8 = undefined;
    @memcpy(bytes[0..ref_len], &children.left.encode());
    @memcpy(bytes[ref_len..], &children.right.encode());
    try txn.put(&key(ref), &bytes);
    return ref;
}
fn insertAt(txn: anytype, current: Ref, incoming: Ref, value_hash: Digest) anyerror!Ref {
    const depth = common(current.prefix, incoming.prefix);
    if (depth < current.depth) {
        try txn.put(&key(incoming), &value_hash);
        return storeBranch(txn, if (bitAt(incoming.prefix, depth)) .{ .left = current, .right = incoming } else .{ .left = incoming, .right = current });
    }
    if (current.depth == 128) {
        const bytes = try txn.get(&key(current));
        if (bytes.len != 32 or !std.meta.eql(leaf(current.prefix, bytes[0..32].*), current)) return error.InvalidRetirementSummary;
        if (!std.meta.eql(current, incoming)) return error.GenerationRetirementChanged;
        return current;
    }
    var children = try loadBranch(txn, current);
    if (bitAt(incoming.prefix, current.depth)) children.right = try insertAt(txn, children.right, incoming, value_hash) else children.left = try insertAt(txn, children.left, incoming, value_hash);
    return storeBranch(txn, children);
}
fn removeAt(txn: anytype, current: Ref, expected: Ref) anyerror!?Ref {
    if (common(current.prefix, expected.prefix) < current.depth) return error.GenerationRetirementChanged;
    if (current.depth == 128) {
        if (!std.meta.eql(current, expected)) return error.GenerationRetirementChanged;
        const bytes = try txn.get(&key(current));
        if (bytes.len != 32 or !std.meta.eql(leaf(current.prefix, bytes[0..32].*), current)) return error.InvalidRetirementSummary;
        try txn.delete(&key(current));
        return null;
    }
    var children = try loadBranch(txn, current);
    const right = bitAt(expected.prefix, current.depth);
    const replacement = try removeAt(txn, if (right) children.right else children.left, expected);
    if (replacement) |ref| {
        if (right) children.right = ref else children.left = ref;
        return try storeBranch(txn, children);
    }
    try txn.delete(&key(current));
    return if (right) children.left else children.right;
}

pub fn add(txn: anytype, generation: Generation, value: []const u8) !void {
    const hash = valueDigest(value);
    const incoming = leaf(generation, hash);
    const after = if (try root(txn)) |before| try insertAt(txn, before, incoming, hash) else blk: {
        try txn.put(&key(incoming), &hash);
        break :blk incoming;
    };
    try storeRoot(txn, after);
}
pub fn remove(txn: anytype, generation: Generation, value: []const u8) !void {
    const before = (try root(txn)) orelse return error.GenerationRetirementChanged;
    try storeRoot(txn, try removeAt(txn, before, leaf(generation, valueDigest(value))));
}
pub fn emptySummary() Summary {
    var digest: Digest = undefined;
    std.crypto.hash.Blake3.hash("retirement-set-empty-v1", &digest, .{});
    return .{ .count = 0, .digest = digest };
}
/// One point read. A missing root is distinct from an initialized empty set;
/// callers must fail closed if old tombstones exist without their summary.
pub fn read(txn: anytype) !?Summary {
    const bytes = txn.get(root_key) catch |err| return if (err == error.NotFound) null else err;
    const present = (try decodeRoot(bytes)) orelse return emptySummary();
    return .{ .count = present.count, .digest = present.digest };
}

const Memory = struct {
    map: std.StringHashMap([]u8),
    reads: usize = 0,
    writes: usize = 0,
    bytes_written: usize = 0,
    fn init() Memory {
        return .{ .map = std.StringHashMap([]u8).init(std.testing.allocator) };
    }
    pub fn deinit(self: *Memory) void {
        var iterator = self.map.iterator();
        while (iterator.next()) |entry| {
            self.map.allocator.free(entry.key_ptr.*);
            self.map.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit();
    }
    pub fn get(self: *Memory, name: []const u8) anyerror![]const u8 {
        self.reads += 1;
        return self.map.get(name) orelse error.NotFound;
    }
    pub fn put(self: *Memory, name: []const u8, value: []const u8) !void {
        const owned = try self.map.allocator.dupe(u8, value);
        errdefer self.map.allocator.free(owned);
        const entry = try self.map.getOrPut(name);
        if (entry.found_existing) self.map.allocator.free(entry.value_ptr.*) else entry.key_ptr.* = try self.map.allocator.dupe(u8, name);
        entry.value_ptr.* = owned;
        self.writes += 1;
        self.bytes_written += name.len + value.len;
    }
    pub fn delete(self: *Memory, name: []const u8) !void {
        if (self.map.fetchRemove(name)) |entry| {
            self.map.allocator.free(entry.key);
            self.map.allocator.free(entry.value);
            self.writes += 1;
            self.bytes_written += name.len;
        }
    }
};
fn testGeneration(index: u64) Generation {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, index, .little);
    var digest: Digest = undefined;
    std.crypto.hash.Blake3.hash(&bytes, &digest, .{});
    return digest[0..16].*;
}

test "compressed retirement summary is canonical and lookup cost independent of history" {
    var forward = Memory.init();
    defer forward.deinit();
    var backward = Memory.init();
    defer backward.deinit();
    const count = 1024;
    for (0..count) |i| {
        try add(&forward, testGeneration(i), "authority");
        try add(&backward, testGeneration(count - i - 1), "authority");
    }
    const expected = (try read(&forward)).?;
    try std.testing.expectEqual(@as(u64, count), expected.count);
    try std.testing.expectEqualDeep(expected, (try read(&backward)).?);
    // N leaves, N-1 compressed branches and one root; no sparse empty nodes.
    try std.testing.expectEqual(@as(u32, 2 * count), forward.map.count());
    const before = forward.reads;
    _ = try read(&forward);
    try std.testing.expectEqual(before + 1, forward.reads);
    // Logical staging writes are a conservative upper bound: a native batch
    // coalesces common ancestor keys inside its transaction before WAL apply.
    try std.testing.expect(forward.writes < count * 20);
    var coalesced_bytes: usize = 0;
    var iterator = forward.map.iterator();
    while (iterator.next()) |entry| coalesced_bytes += entry.key_ptr.*.len + entry.value_ptr.*.len;
    std.debug.print("retirement summary insert n={d} logical_writes={d} logical_bytes={d}\n", .{ count, forward.writes, forward.bytes_written });
    std.debug.print("retirement summary single-batch distinct_keys={d} final_key_value_bytes={d}\n", .{ forward.map.count(), coalesced_bytes });
    const writes_before_gc = forward.writes;
    const bytes_before_gc = forward.bytes_written;
    for (0..count) |i| try remove(&forward, testGeneration(i), "authority");
    try std.testing.expectEqualDeep(emptySummary(), (try read(&forward)).?);
    try std.testing.expectEqual(@as(u32, 1), forward.map.count());
    std.debug.print("retirement summary gc n={d} logical_writes={d} logical_bytes={d}\n", .{ count, forward.writes - writes_before_gc, forward.bytes_written - bytes_before_gc });
}

test "retirement summary binds immutable authority and collapses branches on GC" {
    var store = Memory.init();
    defer store.deinit();
    try std.testing.expect(try read(&store) == null);
    try add(&store, @splat(0), "first");
    const original = (try read(&store)).?;
    try add(&store, @splat(0), "first");
    try std.testing.expectEqualDeep(original, (try read(&store)).?);
    try std.testing.expectError(error.GenerationRetirementChanged, add(&store, @splat(0), "changed"));
    try std.testing.expectError(error.GenerationRetirementChanged, remove(&store, @splat(0), "changed"));
    var adjacent: Generation = @splat(0);
    adjacent[15] = 1;
    try add(&store, adjacent, "second");
    try remove(&store, adjacent, "second");
    try std.testing.expectEqualDeep(original, (try read(&store)).?);
    try std.testing.expectEqual(@as(u32, 2), store.map.count());
    try std.testing.expectError(error.GenerationRetirementChanged, remove(&store, adjacent, "second"));
}

test "retirement summary snapshot copy and abandoned transaction preserve exact roots" {
    var committed = Memory.init();
    defer committed.deinit();
    for (0..64) |i| try add(&committed, testGeneration(i), "authority");
    const before = (try read(&committed)).?;
    var abandoned = Memory.init();
    defer abandoned.deinit();
    var snapshot = Memory.init();
    defer snapshot.deinit();
    var iterator = committed.map.iterator();
    while (iterator.next()) |entry| {
        try abandoned.put(entry.key_ptr.*, entry.value_ptr.*);
        try snapshot.put(entry.key_ptr.*, entry.value_ptr.*);
    }
    try remove(&abandoned, testGeneration(2), "authority");
    try add(&abandoned, testGeneration(100), "different");
    try std.testing.expectEqualDeep(before, (try read(&committed)).?);
    try std.testing.expectEqualDeep(before, (try read(&snapshot)).?);
    // Reopened state supports exact idempotent replay and subsequent GC.
    try add(&snapshot, testGeneration(2), "authority");
    try std.testing.expectEqualDeep(before, (try read(&snapshot)).?);
    for (0..64) |i| try remove(&snapshot, testGeneration(63 - i), "authority");
    try std.testing.expectEqualDeep(emptySummary(), (try read(&snapshot)).?);
    try std.testing.expectEqual(@as(u32, 1), snapshot.map.count());
}

test "retirement summary worst-prefix chain stays bounded and rejects corrupt branches" {
    var store = Memory.init();
    defer store.deinit();
    const zero: Generation = @splat(0);
    try add(&store, zero, "authority");
    for (0..128) |bit| {
        var generation = zero;
        generation[bit / 8] = @as(u8, 0x80) >> @intCast(bit % 8);
        try add(&store, generation, "authority");
    }
    try std.testing.expectEqual(@as(u64, 129), (try read(&store)).?.count);
    try std.testing.expectEqual(@as(u32, 258), store.map.count());
    const before_reads = store.reads;
    try remove(&store, zero, "authority");
    try std.testing.expect(store.reads - before_reads <= 130);
    const before = (try read(&store)).?;
    const root_ref = (try root(&store)).?;
    try store.put(&key(root_ref), "corrupt");
    try std.testing.expectError(error.InvalidRetirementSummary, add(&store, testGeneration(777), "authority"));
    try std.testing.expectEqualDeep(before, (try read(&store)).?);
}

test "retirement summary point read rejects impossible root cardinalities" {
    var store = Memory.init();
    defer store.deinit();
    for ([_]Ref{
        .{ .depth = 0, .prefix = @splat(0), .count = 1, .digest = @splat(7) },
        .{ .depth = 127, .prefix = @splat(0), .count = 3, .digest = @splat(7) },
        .{ .depth = 65, .prefix = @splat(0), .count = std.math.maxInt(u64), .digest = @splat(7) },
    }) |forged| {
        var encoded: [4 + ref_len]u8 = undefined;
        @memcpy(encoded[0..4], "RST1");
        @memcpy(encoded[4..], &forged.encode());
        try store.put(root_key, &encoded);
        const before_reads = store.reads;
        try std.testing.expectError(error.InvalidRetirementSummary, read(&store));
        try std.testing.expectEqual(before_reads + 1, store.reads);
    }
}
