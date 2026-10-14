// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Atomic, binary-safe session changes carried by metadata Raft.
const std = @import("std");
pub const max_bytes = 32 << 20;
pub const max_operations = 4096;
pub const Change = struct { key: []const u8, expected: ?[]const u8, value: ?[]const u8 };
pub const Record = struct { changes: []const Change, scan_epoch: ?u64 = null };
pub fn prefixForGroup(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__api_transaction_sessions__:{x:0>16}:", .{group});
}
pub fn keyAlloc(a: std.mem.Allocator, group: u64, key: []const u8) ![]u8 {
    var buf: [96]u8 = undefined;
    return std.mem.concat(a, u8, &.{ try prefixForGroup(&buf, group), key });
}
pub const epoch_key = "\x00epoch";
fn appendInt(out: *std.ArrayList(u8), a: std.mem.Allocator, comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(a, &bytes);
}
fn appendBytes(out: *std.ArrayList(u8), a: std.mem.Allocator, bytes: ?[]const u8) !void {
    try appendInt(out, a, u32, if (bytes) |value| std.math.cast(u32, value.len) orelse return error.TransactionTooLarge else std.math.maxInt(u32));
    if (bytes) |value| try out.appendSlice(a, value);
}
pub fn encode(a: std.mem.Allocator, record: Record) ![]u8 {
    if (record.changes.len > max_operations) return error.TransactionTooLarge;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.append(a, 1);
    try appendInt(&out, a, u64, record.scan_epoch orelse std.math.maxInt(u64));
    try appendInt(&out, a, u32, @intCast(record.changes.len));
    for (record.changes) |change| {
        if (change.key.len == 0 or change.key.len > 512 or std.mem.eql(u8, change.key, epoch_key)) return error.InvalidTransactionSessionRecord;
        const size = change.key.len +| (if (change.expected) |value| value.len else 0) +| (if (change.value) |value| value.len else 0);
        if (size +| out.items.len +| 12 > max_bytes) return error.TransactionTooLarge;
        try appendBytes(&out, a, change.key);
        try appendBytes(&out, a, change.expected);
        try appendBytes(&out, a, change.value);
    }
    return out.toOwnedSlice(a);
}
const Decoder = struct {
    bytes: []const u8,
    pos: usize = 0,
    fn int(self: *@This(), comptime T: type) !T {
        if (self.bytes.len - self.pos < @sizeOf(T)) return error.InvalidTransactionSessionRecord;
        const result = std.mem.readInt(T, self.bytes[self.pos..][0..@sizeOf(T)], .little);
        self.pos += @sizeOf(T);
        return result;
    }
    fn slice(self: *@This()) !?[]const u8 {
        const size = try self.int(u32);
        if (size == std.math.maxInt(u32)) return null;
        if (size > self.bytes.len - self.pos) return error.InvalidTransactionSessionRecord;
        const result = self.bytes[self.pos..][0..size];
        self.pos += size;
        return result;
    }
};
pub fn decode(a: std.mem.Allocator, bytes: []const u8) !Record {
    if (bytes.len > max_bytes) return error.TransactionTooLarge;
    var reader: Decoder = .{ .bytes = bytes };
    if (try reader.int(u8) != 1) return error.InvalidTransactionSessionRecord;
    const epoch = try reader.int(u64);
    const count = try reader.int(u32);
    if (count > max_operations) return error.TransactionTooLarge;
    const changes = try a.alloc(Change, count);
    errdefer a.free(changes);
    for (changes, 0..) |*change, i| {
        const key = (try reader.slice()) orelse return error.InvalidTransactionSessionRecord;
        if (key.len == 0 or key.len > 512 or std.mem.eql(u8, key, epoch_key)) return error.InvalidTransactionSessionRecord;
        for (changes[0..i]) |previous| if (std.mem.eql(u8, key, previous.key)) return error.InvalidTransactionSessionRecord;
        change.* = .{ .key = key, .expected = try reader.slice(), .value = try reader.slice() };
    }
    if (reader.pos != bytes.len) return error.InvalidTransactionSessionRecord;
    return .{ .changes = changes, .scan_epoch = if (epoch == std.math.maxInt(u64)) null else epoch };
}
pub fn apply(txn: anytype, a: std.mem.Allocator, group: u64, bytes: []const u8) !bool {
    const record = try decode(a, bytes);
    defer a.free(record.changes);
    const epoch_physical = try keyAlloc(a, group, epoch_key);
    defer a.free(epoch_physical);
    const raw_epoch = txn.get(epoch_physical) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const epoch = if (raw_epoch) |value| blk: {
        if (value.len != 8) return error.InvalidTransactionSessionRecord;
        break :blk std.mem.readInt(u64, value[0..8], .little);
    } else 0;
    if (record.scan_epoch) |expected| if (expected != epoch) return false;
    for (record.changes) |change| {
        const key = try keyAlloc(a, group, change.key);
        defer a.free(key);
        const actual = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (change.expected) |expected| {
            if (actual == null or !std.mem.eql(u8, actual.?, expected)) return false;
        } else if (actual != null) return false;
    }
    for (record.changes) |change| {
        const key = try keyAlloc(a, group, change.key);
        defer a.free(key);
        if (change.value) |value| try txn.put(key, value) else try txn.delete(key);
    }
    if (record.changes.len != 0) {
        var next: [8]u8 = undefined;
        std.mem.writeInt(u64, &next, std.math.add(u64, epoch, 1) catch return error.InvalidTransactionSessionRecord, .little);
        try txn.put(epoch_physical, &next);
    }
    return true;
}
test "replicated session records preserve binary values and reject malformed changes" {
    const a = std.testing.allocator;
    const input: Record = .{ .changes = &.{.{ .key = "session", .expected = null, .value = "\xff\x00\x80" }}, .scan_epoch = 7 };
    const bytes = try encode(a, input);
    defer a.free(bytes);
    const output = try decode(a, bytes);
    defer a.free(output.changes);
    try std.testing.expectEqualDeep(input, output);
    for (0..bytes.len) |end| try std.testing.expectError(error.InvalidTransactionSessionRecord, decode(a, bytes[0..end]));
}
test "replicated session compare and set rejects lost updates and scan phantoms atomically" {
    const Map = struct {
        a: std.mem.Allocator,
        values: std.StringHashMapUnmanaged([]u8) = .empty,
        fn deinit(self: *@This()) void {
            var it = self.values.iterator();
            while (it.next()) |entry| {
                self.a.free(entry.key_ptr.*);
                self.a.free(entry.value_ptr.*);
            }
            self.values.deinit(self.a);
        }
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const copy = try self.a.dupe(u8, value);
            errdefer self.a.free(copy);
            if (self.values.getPtr(key)) |old| {
                self.a.free(old.*);
                old.* = copy;
                return;
            }
            const owned_key = try self.a.dupe(u8, key);
            errdefer self.a.free(owned_key);
            try self.values.put(self.a, owned_key, copy);
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            if (self.values.fetchRemove(key)) |entry| {
                self.a.free(entry.key);
                self.a.free(entry.value);
            }
        }
    };
    const a = std.testing.allocator;
    var map: Map = .{ .a = a };
    defer map.deinit();
    const first = try encode(a, .{ .changes = &.{.{ .key = "receipt", .expected = null, .value = "pending" }} });
    defer a.free(first);
    try std.testing.expect(try apply(&map, a, 7, first));
    try std.testing.expect(!try apply(&map, a, 7, first));
    const key = try keyAlloc(a, 7, "receipt");
    defer a.free(key);
    const lost = try encode(a, .{ .changes = &.{ .{ .key = "other", .expected = null, .value = "must not appear" }, .{ .key = "receipt", .expected = "stale", .value = "committed" } } });
    defer a.free(lost);
    try std.testing.expect(!try apply(&map, a, 7, lost));
    const other = try keyAlloc(a, 7, "other");
    defer a.free(other);
    try std.testing.expectError(error.NotFound, map.get(other));
    try std.testing.expectEqualStrings("pending", try map.get(key));
    const phantom = try encode(a, .{ .scan_epoch = 0, .changes = &.{.{ .key = "receipt", .expected = "pending", .value = "committed" }} });
    defer a.free(phantom);
    try std.testing.expect(!try apply(&map, a, 7, phantom));
    const committed = try encode(a, .{ .scan_epoch = 1, .changes = &.{.{ .key = "receipt", .expected = "pending", .value = "committed" }} });
    defer a.free(committed);
    try std.testing.expect(try apply(&map, a, 7, committed));
    try std.testing.expectEqualStrings("committed", try map.get(key));
    try std.testing.expect(try apply(&map, a, 8, first));
}
