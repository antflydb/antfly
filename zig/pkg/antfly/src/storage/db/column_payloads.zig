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
const store = @import("../docstore.zig");
const dv = @import("../../section/typed_doc_values.zig");
const prefix = "\x00\x00__columnar__:blocks:";

/// Generation-local immutable payload identity. Counts belong to durable
/// column metadata, including unpublished staging. Payload and count changes
/// share the metadata transaction; old readers are protected by store MVCC.
pub const Ref = struct {
    digest: [32]u8,
    bytes: u64,
    source_first: u16 = 0,
    source_rows: u16,

    pub fn validate(self: Ref, bytes: []const u8) !void {
        if (bytes.len != self.bytes) return error.InvalidColumnSegment;
        if (bytes.len < 4 or std.hash.Crc32.hash(bytes[0 .. bytes.len - 4]) != std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little)) return error.InvalidColumnSegment;
    }
};

fn hashInt(hash: *std.crypto.hash.Blake3, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    hash.update(&bytes);
}

/// Canonical physical typed identity, independent of compression and block
/// ordinals. Null/presence maps belong to the referencing column descriptor.
pub fn identity(value_type: dv.ValueType, values: []const ?dv.TypedValue) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("ACP1");
    hash.update(&.{@intFromEnum(value_type)});
    for (values, 0..) |maybe_value, row| if (maybe_value) |value| {
        hashInt(&hash, u32, @intCast(row));
        switch (value) {
            .u64_val => |v| hashInt(&hash, u64, v),
            .i64_val => |v| hashInt(&hash, i64, v),
            .f64_val => |v| hashInt(&hash, u64, @bitCast(v)),
            .bool_val => |v| hash.update(&.{@intFromBool(v)}),
            .geo_point => |v| {
                hashInt(&hash, u64, @bitCast(v.lat));
                hashInt(&hash, u64, @bitCast(v.lon));
            },
            .bytes_val => |v| {
                hashInt(&hash, u64, v.len);
                hash.update(v);
            },
            .numeric_val => |v| switch (v) {
                .u64_val => |n| {
                    hash.update(&.{0});
                    hashInt(&hash, u64, n);
                },
                .i64_val => |n| {
                    hash.update(&.{1});
                    hashInt(&hash, i64, n);
                },
                .f64_val => |n| {
                    hash.update(&.{2});
                    hashInt(&hash, u64, @bitCast(n));
                },
            },
        }
    };
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub const Count = struct { references: u64, bytes: u64 };
pub fn decodeCount(bytes: []const u8) !Count {
    if (bytes.len != 20 or std.hash.Crc32.hash(bytes[0..16]) != std.mem.readInt(u32, bytes[16..20], .little)) return error.InvalidColumnSegment;
    const result = Count{ .references = std.mem.readInt(u64, bytes[0..8], .little), .bytes = std.mem.readInt(u64, bytes[8..16], .little) };
    if (result.references == 0 or result.bytes <= 4) return error.InvalidColumnSegment;
    return result;
}
fn encodeCount(count: Count) [20]u8 {
    var result: [20]u8 = undefined;
    std.mem.writeInt(u64, result[0..8], count.references, .little);
    std.mem.writeInt(u64, result[8..16], count.bytes, .little);
    std.mem.writeInt(u32, result[16..20], std.hash.Crc32.hash(result[0..16]), .little);
    return result;
}

pub fn lookup(txn: *store.DocStore.Txn, alloc: std.mem.Allocator, generation: u64, digest: [32]u8, rows: usize) !?Ref {
    const count_key = try key(alloc, generation, digest, true);
    defer alloc.free(count_key);
    const bytes = txn.get(count_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    const count = try decodeCount(bytes);
    return .{ .digest = digest, .bytes = count.bytes, .source_rows = @intCast(rows) };
}

pub fn key(alloc: std.mem.Allocator, generation: u64, digest: [32]u8, count: bool) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}{x:0>16}:{c}:{s}", .{ prefix, generation, @as(u8, if (count) 'q' else 'v'), digest });
}

/// Returns whether an existing payload was shared. Missing source references
/// are corruption, never silently materialized from an unrelated generation.
pub fn retain(txn: *store.DocStore.Txn, alloc: std.mem.Allocator, generation: u64, ref: Ref, encoded: ?[]const u8) !bool {
    const count_key = try key(alloc, generation, ref.digest, true);
    defer alloc.free(count_key);
    const previous = txn.get(count_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    var count: u64 = 0;
    if (previous) |bytes| {
        const decoded = try decodeCount(bytes);
        if (decoded.bytes != ref.bytes) return error.InvalidColumnSegment;
        count = decoded.references;
    } else {
        const bytes = encoded orelse return error.InvalidColumnSegment;
        try ref.validate(bytes);
        const payload_key = try key(alloc, generation, ref.digest, false);
        defer alloc.free(payload_key);
        try txn.put(payload_key, bytes);
    }
    const next = encodeCount(.{ .references = std.math.add(u64, count, 1) catch return error.InvalidColumnSegment, .bytes = ref.bytes });
    try txn.put(count_key, &next);
    return count != 0;
}

pub fn release(txn: *store.DocStore.Txn, alloc: std.mem.Allocator, generation: u64, digest: [32]u8) !void {
    const count_key = try key(alloc, generation, digest, true);
    defer alloc.free(count_key);
    const bytes = try txn.get(count_key);
    const decoded = try decodeCount(bytes);
    const count = decoded.references;
    if (count == 1) {
        const payload_key = try key(alloc, generation, digest, false);
        defer alloc.free(payload_key);
        try txn.delete(payload_key);
        try txn.delete(count_key);
    } else {
        const next = encodeCount(.{ .references = count - 1, .bytes = decoded.bytes });
        try txn.put(count_key, &next);
    }
}
