// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: ELv2
//! Data-only, exact-observation maintenance command. Public requests never
//! supply physical progress bytes or arbitrary metadata writes.
const std = @import("std");

pub const Action = enum { retry, repair };
pub const control_prefix = "\x00\x00__metadata__:relational_index_maintenance:";
pub fn isControlKey(bytes: []const u8) bool {
    return bytes.len == control_prefix.len + 12 and std.mem.startsWith(u8, bytes, control_prefix) and
        std.mem.readInt(u64, bytes[control_prefix.len..][0..8], .big) != 0;
}
pub fn controlKey(id: anytype) [control_prefix.len + 12]u8 {
    var result: [control_prefix.len + 12]u8 = undefined;
    @memcpy(result[0..control_prefix.len], control_prefix);
    std.mem.writeInt(u64, result[control_prefix.len..][0..8], id.generation, .big);
    std.mem.writeInt(u32, result[control_prefix.len + 8 ..][0..4], id.slot, .big);
    return result;
}

/// Replicated desired maintenance, separate from replica-local build progress.
/// One fixed-size receipt per generation bounds repeated-retry storage growth.
pub const Control = struct {
    epoch: u64 = 0,
    last_request: [32]u8 = @splat(0),

    pub fn encode(self: Control, alloc: std.mem.Allocator) ![]u8 {
        const raw = try alloc.alloc(u8, 76);
        @memcpy(raw[0..4], "ARM1");
        std.mem.writeInt(u64, raw[4..12], self.epoch, .little);
        @memcpy(raw[12..44], &self.last_request);
        @memcpy(raw[44..76], &progressDigest(raw[0..44]));
        return raw;
    }

    pub fn decode(raw: []const u8) !Control {
        if (raw.len != 76 or !std.mem.eql(u8, raw[0..4], "ARM1") or
            !std.mem.eql(u8, raw[44..76], &progressDigest(raw[0..44]))) return error.InvalidRelationalIndexProgress;
        return .{ .epoch = std.mem.readInt(u64, raw[4..12], .little), .last_request = raw[12..44].* };
    }
};

pub fn readControl(txn: anytype, id: anytype) !Control {
    const bytes = txn.get(&controlKey(id)) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    return Control.decode(bytes);
}

pub const Command = struct {
    action: Action,
    table_id: u64,
    owner_group_id: u64,
    schema_version: u32,
    index_name: []const u8,
    generation: u64,
    slot: u32,
    owner: [32]u8,
    comparison: [32]u8,
    expected_progress_digest: [32]u8,
    expected_maintenance_epoch: u64,
    routing_key: []const u8,

    pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, stream);
    }

    pub fn validate(self: Command) !void {
        if (self.table_id == 0 or self.owner_group_id == 0 or self.generation == 0 or
            self.index_name.len == 0 or self.index_name.len > 256 or
            !std.unicode.utf8ValidateSlice(self.index_name) or self.routing_key.len > 1024 * 1024)
            return error.InvalidBatchRequest;
    }

    /// Length-framed logical identity, independent of JSON field ordering and
    /// physical progress encoding. Includes the observed progress checksum.
    pub fn fingerprint(self: Command) [32]u8 {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly relational index maintenance command v1");
        for ([_]u64{ @intFromEnum(self.action), self.table_id, self.owner_group_id, self.schema_version, self.generation, self.slot, self.expected_maintenance_epoch, self.index_name.len }) |value| {
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, value, .little);
            state.update(&encoded);
        }
        state.update(self.index_name);
        state.update(&self.owner);
        state.update(&self.comparison);
        state.update(&self.expected_progress_digest);
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, self.routing_key.len, .little);
        state.update(&length);
        state.update(self.routing_key);
        var result: [32]u8 = undefined;
        state.final(&result);
        return result;
    }
};

pub fn progressDigest(bytes: []const u8) [32]u8 {
    var result: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(bytes, &result, .{});
    return result;
}

test "relational index maintenance command identity binds incarnation action and observation" {
    const alloc = std.testing.allocator;
    const command: Command = .{ .action = .retry, .table_id = 7, .owner_group_id = 91, .schema_version = 3, .index_name = "by_id", .generation = 8, .slot = 1, .owner = @splat(2), .comparison = @splat(3), .expected_progress_digest = @splat(4), .expected_maintenance_epoch = 0, .routing_key = "\x00\xff" };
    try command.validate();
    const json = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(json);
    var parsed = try std.json.parseFromSlice(Command, alloc, json, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(command.fingerprint(), parsed.value.fingerprint());
    var changed = command;
    changed.table_id += 1;
    try std.testing.expect(!std.mem.eql(u8, &command.fingerprint(), &changed.fingerprint()));
    changed = command;
    changed.action = .repair;
    try std.testing.expect(!std.mem.eql(u8, &command.fingerprint(), &changed.fingerprint()));
    changed = command;
    changed.expected_progress_digest[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &command.fingerprint(), &changed.fingerprint()));
}

test "relational index maintenance control is checksummed fixed-size desired state" {
    const alloc = std.testing.allocator;
    const state: Control = .{ .epoch = 99, .last_request = @splat(2) };
    const raw = try state.encode(alloc);
    defer alloc.free(raw);
    try std.testing.expectEqualDeep(state, try Control.decode(raw));
    raw[7] ^= 1;
    try std.testing.expectError(error.InvalidRelationalIndexProgress, Control.decode(raw));
    try std.testing.expectError(error.InvalidRelationalIndexProgress, Control.decode(raw[0..75]));
}
