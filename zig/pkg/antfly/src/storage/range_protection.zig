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
pub const bucket_count = 257;
pub const Proof = struct { bucket: u16, generation: ?u64 };

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
        if (!keys.isStoredDocumentRowKey(physical_key)) return;
        if (self.active == null) self.active = try isActive(txn);
        if (!self.active.?) return;
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
