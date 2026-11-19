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

//! Ordered temporal-key claims. All intervals belonging to one equality tuple
//! share its routing digest and one exclusive, intent-only gap fence. The fence
//! is a deletion of an absent metadata key: it never creates a growing group
//! catalog. Physical claims retain canonical ranges and independently owned row
//! identities; lower-bound keys support bounded predecessor/successor probes.
const std = @import("std");
const integrity = @import("relational_integrity_contract.zig");
const ranges = @import("../../sql/range_value.zig");
const binary = @import("../../sql/range_binary.zig");
const keys = @import("../../sql/range_key.zig");
const A = std.mem.Allocator;
pub const max_key_bytes = integrity.key_len + integrity.max_record_bytes + 32;

pub fn guardKey(address: integrity.Address) [integrity.key_len]u8 {
    return address.key(.interval_guard);
}

pub fn prefix(address: integrity.Address) [integrity.key_len]u8 {
    return address.key(.interval);
}

fn ownerDigest(table: []const u8, row: []const u8) integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly temporal key owner v1");
    for ([_][]const u8{ table, row }) |field| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, field.len, .little);
        state.update(&length);
        state.update(field);
    }
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub const Claim = struct {
    tuple: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    schema_version: u32,
    period: ranges.Encoded,

    pub fn view(self: Claim, ctx: *ranges.Context) !binary.View {
        // Schema epoch zero is the initial native table generation. Its
        // identity is fenced by the durable catalog, not a nonzero sentinel.
        if (self.parent_table.len == 0 or self.parent_key.len == 0) return error.InvalidIntegrityRecord;
        const result = try self.period.view(ctx);
        if (result.empty) return error.TemporalEmptyRange;
        return result;
    }

    pub fn writeKey(self: Claim, address: integrity.Address, ctx: *ranges.Context, writer: *std.Io.Writer) !void {
        try address.verifyTuple(self.tuple);
        const period = try self.view(ctx);
        try writer.writeAll(&prefix(address));
        try writer.writeByte(@backingInt(self.period.kind));
        try keys.writeLowerView(ctx, period, writer);
        try writer.writeAll(&ownerDigest(self.parent_table, self.parent_key));
    }

    pub fn keyAlloc(self: Claim, a: A, address: integrity.Address) ![]u8 {
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var context: ranges.Context = .{ .alloc = none.allocator() };
        var counter: std.Io.Writer.Discarding = .init(&.{});
        try self.writeKey(address, &context, &counter.writer);
        if (counter.fullCount() > max_key_bytes) return error.IntegrityRecordTooLarge;
        const result = try a.alloc(u8, @intCast(counter.fullCount()));
        errdefer a.free(result);
        var writer: std.Io.Writer = .fixed(result);
        try self.writeKey(address, &context, &writer);
        return result;
    }

    pub fn encode(self: Claim, a: A, address: integrity.Address) ![]u8 {
        const key = try self.keyAlloc(a, address);
        defer a.free(key);
        var output: std.ArrayList(u8) = .empty;
        errdefer output.deinit(a);
        try output.appendSlice(a, "AFI1");
        try output.append(a, @backingInt(self.period.kind));
        var version: [4]u8 = undefined;
        std.mem.writeInt(u32, &version, self.schema_version, .little);
        try output.appendSlice(a, &version);
        for ([_][]const u8{ self.tuple, self.parent_table, self.parent_key, self.period.bytes }) |field| try integrity.appendField(a, &output, field);
        const result = try integrity.finishRecord(a, &output, key);
        errdefer a.free(result);
        // Every accepted interval must fit a bounded handoff/source-copy
        // record, including its ordered key. Never discover this limit only
        // after a table has become impossible to move or restore.
        if (result.len > integrity.max_record_bytes -| key.len) return error.IntegrityRecordTooLarge;
        return result;
    }

    pub fn decode(key: []const u8, value: []const u8) !Claim {
        if (value.len > integrity.max_record_bytes -| key.len) return error.IntegrityRecordTooLarge;
        const parsed = try integrity.parseKey(key);
        if (parsed.kind != .interval) return error.InvalidIntegrityKey;
        const body = try integrity.recordBody(key, value, "AFI1");
        if (body.len < 5) return error.InvalidIntegrityRecord;
        const kind = std.enums.fromInt(ranges.Kind, body[0]) orelse return error.InvalidIntegrityRecord;
        var reader: integrity.Decoder = .{ .bytes = body[5..] };
        const result: Claim = .{
            .schema_version = std.mem.readInt(u32, body[1..5], .little),
            .tuple = try reader.field(),
            .parent_table = try reader.field(),
            .parent_key = try reader.field(),
            .period = .{ .kind = kind, .bytes = try reader.field() },
        };
        try reader.finish();
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var context: ranges.Context = .{ .alloc = none.allocator() };
        var match: MatchWriter = .{ .expected = key };
        result.writeKey(parsed.address, &context, &match.writer) catch |err| {
            if (err == error.WriteFailed and match.mismatch) return error.IntegrityAddressMismatch;
            return err;
        };
        if (match.position != key.len) return error.IntegrityAddressMismatch;
        return result;
    }

    pub fn memberDigest(self: Claim) !integrity.Digest {
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var context: ranges.Context = .{ .alloc = none.allocator() };
        const period = try self.view(&context);
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly temporal live interval member v1");
        state.update(&integrity.hash(self.tuple));
        state.update(&ownerDigest(self.parent_table, self.parent_key));
        var version: [4]u8 = undefined;
        std.mem.writeInt(u32, &version, self.schema_version, .little);
        state.update(&version);
        try period.hash(&context, &state);
        var result: integrity.Digest = undefined;
        state.final(&result);
        return result;
    }
};

const MatchWriter = struct {
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    expected: []const u8,
    position: usize = 0,
    mismatch: bool = false,
    fn drain(raw: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", raw));
        var consumed: usize = 0;
        for (data, 0..) |part, index| {
            const count = if (index + 1 == data.len) splat else 1;
            for (0..count) |_| {
                if (part.len > self.expected.len -| self.position or !std.mem.eql(u8, part, self.expected[self.position..][0..part.len])) {
                    self.mismatch = true;
                    return error.WriteFailed;
                }
                self.position += part.len;
                consumed += part.len;
            }
        }
        return consumed;
    }
};

test "SQL range temporal claims bind ordered keys ownership and canonical bytes without allocation" {
    const a = std.testing.allocator;
    const Entry = struct { kind: ranges.Kind, input: []const u8, sqlstate: ?[]const u8 };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("../../sql/fixtures/sql_range_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    const address = try integrity.Address.init(@splat(9), "equality-group");
    for (fixture.value.entries) |entry| {
        if (entry.sqlstate != null) continue;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var context: ranges.Context = .{ .alloc = arena.allocator() };
        const logical = try ranges.parseLeaky(&context, entry.kind, entry.input);
        const claim: Claim = .{ .tuple = "equality-group", .parent_table = "products", .parent_key = "row:1", .schema_version = 0, .period = .{ .kind = entry.kind, .bytes = try ranges.encodeLeaky(&context, logical) } };
        if (logical.empty) {
            try std.testing.expectError(error.TemporalEmptyRange, claim.keyAlloc(a, address));
            continue;
        }
        const key = try claim.keyAlloc(a, address);
        defer a.free(key);
        const encoded = try claim.encode(a, address);
        defer a.free(encoded);
        const decoded = try Claim.decode(key, encoded);
        try std.testing.expectEqual(entry.kind, decoded.period.kind);
        try std.testing.expectEqualSlices(u8, claim.period.bytes, decoded.period.bytes);
        try std.testing.expectEqualSlices(u8, &try claim.memberDigest(), &try decoded.memberDigest());
        try std.testing.expectEqualSlices(u8, &address.routing, &try integrity.routingKey(key));
        const expected_lower = try keys.lowerAlloc(&context, logical);
        try std.testing.expectEqualSlices(u8, expected_lower, key[integrity.key_len + 1 .. key.len - 32]);
        const original = key[integrity.key_len];
        key[integrity.key_len] = (original + 1) % 4;
        @memcpy(encoded[encoded.len - 32 ..], &integrity.checksum(key, encoded[0 .. encoded.len - 32]));
        try std.testing.expectError(error.IntegrityAddressMismatch, Claim.decode(key, encoded));
    }
    const guard = guardKey(address);
    try integrity.validateOperation(.{ .routing_key = &address.routing, .key = &guard, .kind = .delete });
    try std.testing.expectError(error.InvalidIntegrityOperation, integrity.validateOperation(.{ .routing_key = &address.routing, .key = &guard, .kind = .guard }));
    try std.testing.expectError(error.InvalidIntegrityRecord, integrity.validateTransferRecord(&guard, ""));
}
