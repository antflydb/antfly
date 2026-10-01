// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable logical range generations. Activation is an explicit replicated
//! capability transition, never a side effect of opening a database or reading.
const std = @import("std");
const keys = @import("internal_keys.zig");

pub const activation_key = "\x00\x00__metadata__:range_tracking";
pub const activation_value = "ARG1";
pub const counter_prefix = "\x00\x00__metadata__:range_generation:";
pub const writer_prefix = "\x00\x00__metadata__:range_writer:";
pub const index_counter_prefix = "\x00\x00__metadata__:index_span_generation:";
pub const index_writer_prefix = "\x00\x00__metadata__:index_span_writer:";
const index_forward_prefix = "\x00\x00R\x01";
pub const index_id_bytes = 12;
pub const index_span_digest_bytes = 16;
pub const bucket_count = 257;
pub const index_bucket_sentinel: u16 = bucket_count;
pub const IndexSpan = struct {
    id: [index_id_bytes]u8,
    digest: [index_span_digest_bytes]u8,
};
pub const Proof = struct {
    bucket: u16,
    generation: ?u64,
    index: ?IndexSpan = null,

    pub fn jsonStringify(self: Proof, writer: anytype) !void {
        // Existing broad scans carry up to 257 primary proofs. Keep their
        // established compact wire shape; only exact index probes need a tag.
        if (self.index) |span| {
            try writer.write(.{ .bucket = self.bucket, .generation = self.generation, .index = span });
        } else {
            try writer.write(.{ .bucket = self.bucket, .generation = self.generation });
        }
    }
};

pub fn validateProof(proof: Proof) !void {
    if (proof.index) |span| {
        if (proof.bucket != index_bucket_sentinel or std.mem.readInt(u64, span.id[0..8], .big) == 0) return error.InvalidRangeTrackingProof;
    } else if (proof.bucket >= bucket_count) return error.InvalidRangeTrackingProof;
}

pub fn proofLess(a: Proof, b: Proof) bool {
    if (a.bucket != b.bucket) return a.bucket < b.bucket;
    const ai = a.index orelse return false;
    const bi = b.index orelse return true;
    const id_order = std.mem.order(u8, &ai.id, &bi.id);
    return if (id_order == .eq) std.mem.order(u8, &ai.digest, &bi.digest) == .lt else id_order == .lt;
}

pub fn validateRequest(req: anytype) !void {
    if (req.range_guards.len != 0 and (req.transaction == null or req.transaction.? != .prepare)) return error.InvalidBatchRequest;
    if (!req.activate_range_tracking) return;
    const defaults: @TypeOf(req) = .{};
    inline for (std.meta.fields(@TypeOf(req))) |field| {
        if (comptime std.mem.eql(u8, field.name, "activate_range_tracking") or std.mem.eql(u8, field.name, "timestamp_ns") or std.mem.eql(u8, field.name, "sync_level") or std.mem.eql(u8, field.name, "schema_version")) continue;
        const value = @field(req, field.name);
        if (comptime @typeInfo(field.type) == .pointer and @typeInfo(field.type).pointer.size == .slice) {
            if (value.len != 0) return error.InvalidBatchRequest;
        } else if (!std.meta.eql(value, @field(defaults, field.name))) return error.InvalidBatchRequest;
    }
}

pub fn bucket(logical_key: []const u8) u16 {
    return if (logical_key.len == 0) 0 else 1 + @as(u16, logical_key[0]);
}
pub fn counterKey(id: u16) [counter_prefix.len + 2]u8 {
    std.debug.assert(id < bucket_count);
    var out: [counter_prefix.len + 2]u8 = undefined;
    @memcpy(out[0..counter_prefix.len], counter_prefix);
    std.mem.writeInt(u16, out[counter_prefix.len..][0..2], id, .big);
    return out;
}
pub fn writerKey(id: u16) [writer_prefix.len + 2]u8 {
    std.debug.assert(id < bucket_count);
    var out: [writer_prefix.len + 2]u8 = undefined;
    @memcpy(out[0..writer_prefix.len], writer_prefix);
    std.mem.writeInt(u16, out[writer_prefix.len..][0..2], id, .big);
    return out;
}
pub fn counterBucket(key: []const u8) ?u16 {
    if (key.len != counter_prefix.len + 2 or !std.mem.startsWith(u8, key, counter_prefix)) return null;
    const id = std.mem.readInt(u16, key[counter_prefix.len..][0..2], .big);
    return if (id < bucket_count) id else null;
}
/// The full encoded tuple, including the immutable index generation, is the
/// exact-equality span identity. Hash collisions only add false conflicts:
/// both colliding tuples update the same durable counter.
pub fn indexSpanDigest(forward_key: []const u8) !?[index_span_digest_bytes]u8 {
    if (!std.mem.startsWith(u8, forward_key, index_forward_prefix)) return null;
    const start = index_forward_prefix.len + index_id_bytes;
    if (forward_key.len < start + 7) return error.InvalidRelationalIndexForwardKey;
    const end = forward_key.len - 4;
    const document_size = std.mem.readInt(u32, forward_key[end..][0..4], .big);
    if (document_size < 2 or document_size >= end - start) return error.InvalidRelationalIndexForwardKey;
    const tuple_end = end - document_size;
    const terminator = keys.findComponentTerminator(forward_key, tuple_end) orelse return error.InvalidRelationalIndexForwardKey;
    if (terminator + 2 != end) return error.InvalidRelationalIndexForwardKey;
    return try indexTupleSpanDigest(forward_key[index_forward_prefix.len..start][0..index_id_bytes].*, forward_key[start..tuple_end]);
}

/// The planner already has encoded tuple bytes before a forward key is built.
/// Share the identity algorithm with committed effects and old reverse tuples.
pub fn indexTupleSpanDigest(index_id: [index_id_bytes]u8, tuple: []const u8) ![index_span_digest_bytes]u8 {
    if (std.mem.readInt(u64, index_id[0..8], .big) == 0 or tuple.len == 0) return error.InvalidRelationalIndexForwardKey;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:index-span:v1\x00");
    hash.update(&index_id);
    hash.update(tuple);
    var full: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hash.final(&full);
    return full[0..index_span_digest_bytes].*;
}
pub fn indexCounterKey(digest: [index_span_digest_bytes]u8) [index_counter_prefix.len + index_span_digest_bytes]u8 {
    var result: [index_counter_prefix.len + index_span_digest_bytes]u8 = undefined;
    @memcpy(result[0..index_counter_prefix.len], index_counter_prefix);
    @memcpy(result[index_counter_prefix.len..], &digest);
    return result;
}
pub fn indexWriterKey(digest: [index_span_digest_bytes]u8) [index_writer_prefix.len + index_span_digest_bytes]u8 {
    var result: [index_writer_prefix.len + index_span_digest_bytes]u8 = undefined;
    @memcpy(result[0..index_writer_prefix.len], index_writer_prefix);
    @memcpy(result[index_writer_prefix.len..], &digest);
    return result;
}
pub fn indexCounterDigest(key: []const u8) ?[index_span_digest_bytes]u8 {
    if (key.len != index_counter_prefix.len + index_span_digest_bytes or !std.mem.startsWith(u8, key, index_counter_prefix)) return null;
    return key[index_counter_prefix.len..][0..index_span_digest_bytes].*;
}
pub fn indexGeneration(txn: anytype, digest: [index_span_digest_bytes]u8) !?u64 {
    const key = indexCounterKey(digest);
    const bytes = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len != 8) return error.InvalidRangeTrackingState;
    return std.mem.readInt(u64, bytes[0..8], .little);
}
pub fn isActive(txn: anytype) !bool {
    const bytes = txn.get(activation_key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (!std.mem.eql(u8, bytes, activation_value)) return error.InvalidRangeTrackingState;
    return true;
}
pub fn generation(txn: anytype, id: u16) !?u64 {
    const key = counterKey(id);
    const bytes = txn.get(&key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len != 8) return error.InvalidRangeTrackingState;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

/// One cached activation probe and one counter increment per changed bucket,
/// regardless of rows in the native write transaction. Counter writes recurse
/// through the normal transaction wrapper but are not primary document keys.
pub const Mutation = struct {
    active: ?bool = null,
    touched: std.StaticBitSet(bucket_count) = .initEmpty(),

    pub fn touch(self: *Mutation, txn: anytype, physical_key: []const u8) anyerror!void {
        const primary = keys.isStoredDocumentRowKey(physical_key);
        const forward_index = !primary and std.mem.startsWith(u8, physical_key, index_forward_prefix);
        if (!primary and !forward_index) return;
        if (self.active == null) self.active = try isActive(txn);
        if (!self.active.?) return;
        if (forward_index) {
            const digest = (try indexSpanDigest(physical_key)).?;
            const key = indexCounterKey(digest);
            const current = try indexGeneration(txn, digest) orelse 0;
            const next = std.math.add(u64, current, 1) catch return error.RangeTrackingGenerationExhausted;
            var value: [8]u8 = undefined;
            std.mem.writeInt(u64, &value, next, .little);
            try txn.put(&key, &value);
            return;
        }
        // Memcomparable component encoding preserves the first logical byte;
        // 00 00 terminates an empty component, while 00 ff is a leading NUL.
        const id: u16 = if (physical_key[1] == 0 and physical_key[2] == 0) 0 else 1 + @as(u16, physical_key[1]);
        if (self.touched.isSet(id)) return;
        const current = try generation(txn, id) orelse 0;
        const next = std.math.add(u64, current, 1) catch return error.RangeTrackingGenerationExhausted;
        const key = counterKey(id);
        var value: [8]u8 = undefined;
        std.mem.writeInt(u64, &value, next, .little);
        try txn.put(&key, &value);
        self.touched.set(id);
    }
};

/// Capture conservatively intersecting first-byte buckets from the same read
/// transaction as row data. Inclusive/exclusive endpoint differences only add
/// conservative conflicts, never omit a phantom at a boundary.
pub fn capture(alloc: std.mem.Allocator, txn: anytype, from: []const u8, to: []const u8) ![]Proof {
    if (!try isActive(txn)) return error.SqlRangeTrackingRequired;
    const first = bucket(from);
    const last = if (to.len == 0) bucket_count - 1 else bucket(to);
    if (first > last) return error.InvalidRangeTrackingState;
    const proofs = try alloc.alloc(Proof, last - first + 1);
    errdefer alloc.free(proofs);
    for (proofs, first..) |*proof, id| proof.* = .{ .bucket = @intCast(id), .generation = try generation(txn, @intCast(id)) };
    return proofs;
}

/// Exact full-tuple index observations use the same retained read snapshot as
/// the matching entries. The index ID pins its immutable generation; prepare
/// separately checks that it is still READY in the owner's current catalog.
pub fn captureIndex(alloc: std.mem.Allocator, txn: anytype, id: [index_id_bytes]u8, tuple: []const u8) ![]Proof {
    if (!try isActive(txn)) return error.SqlRangeTrackingRequired;
    const digest = try indexTupleSpanDigest(id, tuple);
    const proofs = try alloc.alloc(Proof, 1);
    errdefer alloc.free(proofs);
    proofs[0] = .{ .bucket = index_bucket_sentinel, .generation = try indexGeneration(txn, digest), .index = .{ .id = id, .digest = digest } };
    return proofs;
}

test "range tracking inactive and same bucket mutation work is constant per batch" {
    const Probe = struct {
        active: bool,
        gets: usize = 0,
        puts: usize = 0,
        value: [8]u8 = @splat(0),
        fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            self.gets += 1;
            if (std.mem.eql(u8, key, activation_key)) return if (self.active) activation_value else error.NotFound;
            return &self.value;
        }
        fn put(self: *@This(), _: []const u8, value: []const u8) !void {
            self.puts += 1;
            @memcpy(&self.value, value);
        }
    };
    const physical = try keys.documentKeyAlloc(std.testing.allocator, "doc:123");
    defer std.testing.allocator.free(physical);
    inline for (.{ false, true }) |active| {
        var probe: Probe = .{ .active = active };
        var mutation: Mutation = .{};
        const start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
        for (0..100_000) |_| try mutation.touch(&probe, physical);
        const elapsed = std.Io.Clock.awake.now(std.testing.io).nanoseconds - start;
        try std.testing.expectEqual(@as(usize, if (active) 2 else 1), probe.gets);
        try std.testing.expectEqual(@as(usize, if (active) 1 else 0), probe.puts);
        std.debug.print("range tracking active={any} touches=100000 probes={d} counter_writes={d} elapsed_ns={d}\n", .{ active, probe.gets, probe.puts, elapsed });
    }
    // This is deliberately a conservative prefix scheme, not an adaptive
    // interval index. Common-prefix keys share conflicts even when distinct.
    try std.testing.expectEqual(bucket("doc:1"), bucket("doc:999999"));
    try std.testing.expect(bucket("doc:1") != bucket("user:1"));
}

test "range tracking index span identity ignores document suffix and touches durable counter" {
    const alloc = std.testing.allocator;
    const IndexId = @import("relational_index.zig").RelationalIndexId;
    const Fixture = struct {
        fn forward(owner: std.mem.Allocator, id: IndexId, tuple: []const u8, document: []const u8) ![]u8 {
            var component: std.ArrayList(u8) = .empty;
            defer component.deinit(owner);
            try keys.appendDocumentPrefix(&component, owner, document);
            var out: std.ArrayList(u8) = .empty;
            errdefer out.deinit(owner);
            try out.appendSlice(owner, index_forward_prefix);
            try out.appendSlice(owner, &id.encode());
            try out.appendSlice(owner, tuple);
            try out.appendSlice(owner, component.items[1..]);
            var footer: [4]u8 = undefined;
            std.mem.writeInt(u32, &footer, @intCast(component.items.len - 1), .big);
            try out.appendSlice(owner, &footer);
            return out.toOwnedSlice(owner);
        }
    };
    const tuple = [_]u8{ 0x80, 'x', 0, 0 };
    const first = try Fixture.forward(alloc, .{ .generation = 7, .slot = 2 }, &tuple, "row-a");
    defer alloc.free(first);
    const second = try Fixture.forward(alloc, .{ .generation = 7, .slot = 2 }, &tuple, "row-b");
    defer alloc.free(second);
    const other = try Fixture.forward(alloc, .{ .generation = 7, .slot = 3 }, &tuple, "row-a");
    defer alloc.free(other);
    const span = (try indexSpanDigest(first)).?;
    const other_span = (try indexSpanDigest(other)).?;
    try std.testing.expectEqual(span, try indexTupleSpanDigest((IndexId{ .generation = 7, .slot = 2 }).encode(), &tuple));
    try std.testing.expectEqual(span, (try indexSpanDigest(second)).?);
    try std.testing.expect(!std.mem.eql(u8, &span, &other_span));
    const counter_key = indexCounterKey(span);
    const writer_key = indexWriterKey(span);
    try std.testing.expectEqual(span, indexCounterDigest(&counter_key).?);
    try std.testing.expect(indexCounterDigest(&writer_key) == null);
    var corrupt = try alloc.dupe(u8, first);
    defer alloc.free(corrupt);
    @memset(corrupt[corrupt.len - 4 ..], 0xff);
    try std.testing.expectError(error.InvalidRelationalIndexForwardKey, indexSpanDigest(corrupt));

    const Probe = struct {
        expected: [index_span_digest_bytes]u8,
        active: bool = true,
        reads: usize = 0,
        writes: usize = 0,
        counter: [8]u8 = undefined,
        fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            self.reads += 1;
            if (std.mem.eql(u8, key, activation_key)) return if (self.active) activation_value else error.NotFound;
            if (indexCounterDigest(key) == null or self.writes == 0) return error.NotFound;
            return &self.counter;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try std.testing.expectEqual(self.expected, indexCounterDigest(key).?);
            self.writes += 1;
            @memcpy(&self.counter, value);
        }
    };
    var probe: Probe = .{ .expected = span };
    const before = try captureIndex(alloc, &probe, (IndexId{ .generation = 7, .slot = 2 }).encode(), &tuple);
    defer alloc.free(before);
    try std.testing.expectEqual(@as(usize, 1), before.len);
    try std.testing.expectEqual(@as(?u64, null), before[0].generation);
    try std.testing.expectEqual(span, before[0].index.?.digest);
    try validateProof(before[0]);
    var mutation: Mutation = .{};
    try mutation.touch(&probe, first);
    try mutation.touch(&probe, second);
    try std.testing.expectEqual(@as(usize, 2), probe.writes);
    try std.testing.expectEqual(@as(u64, 2), std.mem.readInt(u64, &probe.counter, .little));
    const after = try captureIndex(alloc, &probe, (IndexId{ .generation = 7, .slot = 2 }).encode(), &tuple);
    defer alloc.free(after);
    try std.testing.expectEqual(@as(?u64, 2), after[0].generation);
    var inactive: Probe = .{ .expected = span, .active = false };
    var ignored: Mutation = .{};
    try ignored.touch(&inactive, corrupt);
    try ignored.touch(&inactive, first);
    try std.testing.expectEqual(@as(usize, 0), inactive.writes);
}
