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
        if (bytes.len < 4 or @import("antfly_hash").Crc32.hash(bytes[0 .. bytes.len - 4]) != std.mem.readInt(u32, bytes[bytes.len - 4 ..][0..4], .little)) return error.InvalidColumnSegment;
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

/// Prepared only by the table's single column-maintenance owner. Namespace
/// fencing and the build token must be checked before applying the resulting
/// writes. Foreground mutations never edit payload ownership counts.
pub const Delta = struct {
    digest: [32]u8,
    bytes: ?u64 = null,
    encoded: ?[]const u8 = null,
    retains: u64 = 0,
    releases: u64 = 0,
};

pub const Prepared = struct {
    writes: std.ArrayListUnmanaged(store.KVPair) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    shared: u64 = 0,
    new_bytes: u64 = 0,

    pub fn apply(self: Prepared, txn: *store.DocStore.Txn) !void {
        for (self.writes.items) |write| try txn.put(write.key, write.value);
        for (self.deletes.items) |name| try txn.delete(name);
    }
};

/// All output and temporary storage belongs to the caller's preparation arena.
/// Aggregate before reading: one count lookup/update per distinct digest,
/// independent of the number of column descriptors sharing that payload.
pub fn prepare(target: *store.DocStore, alloc: std.mem.Allocator, generation: u64, deltas: []const Delta) !Prepared {
    var result: Prepared = .{};
    if (deltas.len == 0) return result;
    const sorted = try alloc.dupe(Delta, deltas);
    std.mem.sort(Delta, sorted, {}, struct {
        fn less(_: void, a: Delta, b: Delta) bool {
            return std.mem.order(u8, &a.digest, &b.digest) == .lt;
        }
    }.less);
    var unique: usize = 0;
    for (sorted) |delta| {
        if (unique != 0 and std.mem.eql(u8, &sorted[unique - 1].digest, &delta.digest)) {
            const previous = &sorted[unique - 1];
            if (previous.bytes != null and delta.bytes != null and previous.bytes.? != delta.bytes.?) return error.InvalidColumnSegment;
            previous.bytes = previous.bytes orelse delta.bytes;
            previous.encoded = previous.encoded orelse delta.encoded;
            previous.retains = std.math.add(u64, previous.retains, delta.retains) catch return error.InvalidColumnSegment;
            previous.releases = std.math.add(u64, previous.releases, delta.releases) catch return error.InvalidColumnSegment;
        } else {
            sorted[unique] = delta;
            unique += 1;
        }
    }
    var offset: usize = 0;
    while (offset < unique) {
        const end = @min(offset + 128, unique);
        var probe = try target.beginProbeTxn();
        defer probe.abort();
        var names: [128][]const u8 = undefined;
        var values: [128]?[]const u8 = undefined;
        for (sorted[offset..end], 0..) |delta, i| names[i] = try key(alloc, generation, delta.digest, true);
        try probe.getManySorted(names[0 .. end - offset], values[0 .. end - offset]);
        for (sorted[offset..end], names[0 .. end - offset], values[0 .. end - offset]) |delta, name, value| {
            const previous: ?Count = if (value) |bytes| try decodeCount(bytes) else null;
            if (previous) |count| if (delta.bytes) |bytes| {
                if (bytes != count.bytes) return error.InvalidColumnSegment;
            };
            if (previous == null and delta.releases != 0) return error.InvalidColumnSegment;
            const added = std.math.add(u64, if (previous) |count| count.references else 0, delta.retains) catch return error.InvalidColumnSegment;
            const references = std.math.sub(u64, added, delta.releases) catch return error.InvalidColumnSegment;
            if (references == 0) {
                if (previous != null) {
                    try result.deletes.append(alloc, try key(alloc, generation, delta.digest, false));
                    try result.deletes.append(alloc, name);
                }
                continue;
            }
            const bytes = if (previous) |count| count.bytes else delta.bytes orelse return error.InvalidColumnSegment;
            if (previous == null) {
                const encoded = delta.encoded orelse return error.InvalidColumnSegment;
                try (Ref{ .digest = delta.digest, .bytes = bytes, .source_rows = 0 }).validate(encoded);
                try result.writes.append(alloc, .{ .key = try key(alloc, generation, delta.digest, false), .value = encoded });
                result.new_bytes += bytes;
            }
            result.shared += delta.retains -| @as(u64, if (previous == null) 1 else 0);
            const count = encodeCount(.{ .references = references, .bytes = bytes });
            try result.writes.append(alloc, .{ .key = name, .value = try alloc.dupe(u8, &count) });
        }
        offset = end;
    }
    return result;
}
pub fn decodeCount(bytes: []const u8) !Count {
    if (bytes.len != 20 or @import("antfly_hash").Crc32.hash(bytes[0..16]) != std.mem.readInt(u32, bytes[16..20], .little)) return error.InvalidColumnSegment;
    const result = Count{ .references = std.mem.readInt(u64, bytes[0..8], .little), .bytes = std.mem.readInt(u64, bytes[8..16], .little) };
    if (result.references == 0 or result.bytes <= 4) return error.InvalidColumnSegment;
    return result;
}
fn encodeCount(count: Count) [20]u8 {
    var result: [20]u8 = undefined;
    std.mem.writeInt(u64, result[0..8], count.references, .little);
    std.mem.writeInt(u64, result[8..16], count.bytes, .little);
    std.mem.writeInt(u32, result[16..20], @import("antfly_hash").Crc32.hash(result[0..16]), .little);
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
