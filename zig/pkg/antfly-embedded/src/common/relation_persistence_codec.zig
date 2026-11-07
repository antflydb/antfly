// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fixed-size native catalog encoding. User row payloads and JSON parsing do
//! not belong in the process ownership descriptor or borrowed point projection.
const std = @import("std");
const persistence = @import("relation_persistence.zig");
pub const descriptor_bytes = 43;
pub const owner_bytes = 112;
pub const Encoded = struct {
    bytes: [descriptor_bytes + owner_bytes]u8 = @splat(0),
    len: usize = 0,
    pub fn slice(self: *const Encoded) []const u8 {
        return self.bytes[0..self.len];
    }
};

/// Native primary catalogs encode their transactional facts into stack
/// storage. Reserve the exact maximum once; escaping it is a hard error.
pub fn encodeFixed(value: persistence.Descriptor) !Encoded {
    var result: Encoded = .{};
    try value.validate();
    result.len = descriptor_bytes + (if (value.owner != null) @as(usize, owner_bytes) else 0);
    var writer: std.Io.Writer = .fixed(result.bytes[0..result.len]);
    try writer.writeByte(value.version);
    try writer.writeByte(if (value.kind == .temporary) 1 else 2);
    try writer.writeInt(u64, value.owner_node_id, .little);
    try writer.writeInt(u64, value.owner_store_id, .little);
    try writer.writeAll(&value.generation_instance);
    try writer.writeInt(u64, value.data_generation, .little);
    try writer.writeByte(switch (value.on_commit) {
        .preserve_rows => 0,
        .delete_rows => 1,
        .drop => 2,
    });
    if (value.owner) |owner| {
        try writer.writeInt(u64, owner.issuer_node_id, .little);
        try writer.writeAll(&owner.connection_id);
        try writer.writeAll(&owner.wire_epoch);
        try writer.writeAll(&owner.execution_instance);
        try writer.writeInt(u64, owner.connection_generation, .little);
        try writer.writeAll(&owner.principal_digest);
    }
    std.debug.assert(writer.end == result.len);
    return result;
}

fn integer(a: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: u64) !void {
    var buffer: [8]u8 = undefined;
    std.mem.writeInt(u64, &buffer, value, .little);
    try out.appendSlice(a, &buffer);
}

pub fn append(a: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: persistence.Descriptor) !void {
    try value.validate();
    try out.ensureUnusedCapacity(a, descriptor_bytes + (if (value.owner != null) @as(usize, owner_bytes) else 0));
    try out.append(a, value.version);
    try out.append(a, if (value.kind == .temporary) 1 else 2);
    try integer(a, out, value.owner_node_id);
    try integer(a, out, value.owner_store_id);
    try out.appendSlice(a, &value.generation_instance);
    try integer(a, out, value.data_generation);
    try out.append(a, switch (value.on_commit) {
        .preserve_rows => 0,
        .delete_rows => 1,
        .drop => 2,
    });
    if (value.owner) |owner| {
        try integer(a, out, owner.issuer_node_id);
        try out.appendSlice(a, &owner.connection_id);
        try out.appendSlice(a, &owner.wire_epoch);
        try out.appendSlice(a, &owner.execution_instance);
        try integer(a, out, owner.connection_generation);
        try out.appendSlice(a, &owner.principal_digest);
    }
}

fn take(bytes: []const u8, pos: *usize, comptime count: usize) ![count]u8 {
    if (pos.* > bytes.len or count > bytes.len - pos.*) return error.InvalidSqlRelationPersistence;
    const result = bytes[pos.*..][0..count].*;
    pos.* += count;
    return result;
}
fn readInteger(bytes: []const u8, pos: *usize) !u64 {
    return std.mem.readInt(u64, &try take(bytes, pos, 8), .little);
}

pub fn read(bytes: []const u8, pos: *usize) !persistence.Descriptor {
    const version = (try take(bytes, pos, 1))[0];
    const kind: persistence.Kind = switch ((try take(bytes, pos, 1))[0]) {
        1 => .temporary,
        2 => .unlogged,
        else => return error.InvalidSqlRelationPersistence,
    };
    var value: persistence.Descriptor = .{
        .version = version,
        .kind = kind,
        .owner_node_id = try readInteger(bytes, pos),
        .owner_store_id = try readInteger(bytes, pos),
        .generation_instance = try take(bytes, pos, 16),
        .data_generation = try readInteger(bytes, pos),
        .on_commit = switch ((try take(bytes, pos, 1))[0]) {
            0 => .preserve_rows,
            1 => .delete_rows,
            2 => .drop,
            else => return error.InvalidSqlRelationPersistence,
        },
    };
    if (kind == .temporary) value.owner = .{
        .issuer_node_id = try readInteger(bytes, pos),
        .connection_id = try take(bytes, pos, 32),
        .wire_epoch = try take(bytes, pos, 16),
        .execution_instance = try take(bytes, pos, 16),
        .connection_generation = try readInteger(bytes, pos),
        .principal_digest = try take(bytes, pos, 32),
    };
    try value.validate();
    return value;
}

test "SQL relation persistence native codec preserves full width IDs distinct epochs and rejects every truncated image" {
    const descriptor: persistence.Descriptor = .{
        .kind = .temporary,
        .owner_node_id = std.math.maxInt(u64) - 1,
        .owner_store_id = std.math.maxInt(u64) - 2,
        .generation_instance = @splat(7),
        .data_generation = std.math.maxInt(u64) - 3,
        .on_commit = .delete_rows,
        .owner = .{ .issuer_node_id = 5, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = std.math.maxInt(u64) - 4, .principal_digest = @splat(4) },
    };
    var bytes: std.ArrayListUnmanaged(u8) = .empty;
    defer bytes.deinit(std.testing.allocator);
    try append(std.testing.allocator, &bytes, descriptor);
    const fixed = try encodeFixed(descriptor);
    try std.testing.expectEqualSlices(u8, bytes.items, fixed.slice());
    try std.testing.expectEqual(@as(usize, descriptor_bytes + owner_bytes), bytes.items.len);
    var pos: usize = 0;
    try std.testing.expectEqualDeep(descriptor, try read(bytes.items, &pos));
    try std.testing.expectEqual(bytes.items.len, pos);
    for (0..bytes.items.len) |length| {
        pos = 0;
        try std.testing.expectError(error.InvalidSqlRelationPersistence, read(bytes.items[0..length], &pos));
    }
    bytes.items[0] = 2;
    pos = 0;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, read(bytes.items, &pos));
}
