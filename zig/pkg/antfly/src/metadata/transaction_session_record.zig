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

//! Atomic, binary-safe session changes carried by metadata Raft.
const std = @import("std");
pub const count_key = "\x00session-count-v1";
pub const session_prefix = "\x00\x00__api_txn_sessions__:";
pub const bound_session_header = "{\"session_format\":2,";
fn frameVersion(changes: []const Change) u8 {
    for (changes) |change| if (change.value_is_splice) return 4;
    for (changes) |change| if (change.expected_is_digest) return 3;
    for (changes) |change| {
        if (!std.mem.startsWith(u8, change.key, session_prefix)) continue;
        if (change.value) |value| if (std.mem.startsWith(u8, value, bound_session_header)) return 2;
    }
    return 1;
}

pub const max_bytes = 32 << 20;
pub const max_receipt_growth = 64 << 10;
pub const max_operations = 4096;
const Digest = std.crypto.hash.Blake3;
pub const Change = struct {
    key: []const u8,
    expected: ?[]const u8,
    value: ?[]const u8,
    expected_is_digest: bool = false,
    value_is_splice: bool = false,
    observation_only: bool = false, // Local writer state, never serialized.
};
pub const Record = struct { changes: []const Change, scan_epoch: ?u64 = null };
pub fn prefixForGroup(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__api_transaction_sessions__:{x:0>16}:", .{group});
}
pub fn keyAlloc(a: std.mem.Allocator, group: u64, key: []const u8) ![]u8 {
    var buf: [96]u8 = undefined;
    return std.mem.concat(a, u8, &.{ try prefixForGroup(&buf, group), key });
}
pub const epoch_key = "\x00epoch";
// Keep these physical keys inside the group's snapshot projection, outside
// its logical API. Old binaries advance epoch_key on every successful write.
// A discontinuity starts a new generation, invalidating every pre-gap prefix.
pub const cache_namespace = "\x00reader_v1/";
const count_watermark = cache_namespace ++ "count-epoch";
const watermark_key = cache_namespace ++ "epoch";
const entry_prefix = cache_namespace ++ "record/";
pub const cached_prefix_bytes = 128;
pub fn internalKey(key: []const u8) bool {
    return std.mem.eql(u8, key, epoch_key) or std.mem.startsWith(u8, key, cache_namespace);
}

fn cacheKey(a: std.mem.Allocator, group: u64, logical: []const u8) ![]u8 {
    var prefix: [96]u8 = undefined;
    return std.mem.concat(a, u8, &.{ try prefixForGroup(&prefix, group), entry_prefix, logical[session_prefix.len..] });
}

fn optionalGet(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

// Older metadata binaries still advance epoch_key. Never trust a count
// across an unobserved apply. The next capped BEGIN bootstraps from the
// actual session prefix under a scan fence.
pub fn countIsCurrent(txn: anytype, a: std.mem.Allocator, group: u64, epoch: u64) !bool {
    const key = try keyAlloc(a, group, count_watermark);
    defer a.free(key);
    const stamp = (try optionalGet(txn, key)) orelse return false;
    if (stamp.len != 8) return error.InvalidTransactionSessionRecord;
    return std.mem.readInt(u64, stamp[0..8], .little) == epoch;
}

fn generation(txn: anytype, marker_key: []const u8, epoch: u64) !?u64 {
    const marker = (try optionalGet(txn, marker_key)) orelse return null;
    if (marker.len != 16) return error.InvalidTransactionSessionRecord;
    if (std.mem.readInt(u64, marker[0..8], .little) != epoch) return null;
    return std.mem.readInt(u64, marker[8..16], .little);
}

pub fn cachedRange(scope: anytype, a: std.mem.Allocator, group: u64, key: []const u8, offset: u64, length: usize) !?[]const u8 {
    if (!std.mem.startsWith(u8, key, session_prefix) or offset > cached_prefix_bytes or length > cached_prefix_bytes - offset) return null;
    const epoch_key_bytes = try keyAlloc(a, group, epoch_key);
    defer a.free(epoch_key_bytes);
    const epoch_bytes = (try optionalGet(scope, epoch_key_bytes)) orelse return null;
    if (epoch_bytes.len != 8) return error.InvalidTransactionSessionRecord;
    const epoch = std.mem.readInt(u64, epoch_bytes[0..8], .little);
    const marker_key = try keyAlloc(a, group, watermark_key);
    defer a.free(marker_key);
    const gen = (try generation(scope, marker_key, epoch)) orelse return null;
    const entry_key = try cacheKey(a, group, key);
    defer a.free(entry_key);
    const entry = (try optionalGet(scope, entry_key)) orelse return null;
    if (entry.len < 8 or entry.len > 8 + cached_prefix_bytes) return error.InvalidTransactionSessionRecord;
    if (std.mem.readInt(u64, entry[0..8], .little) != gen) return null;
    const start: usize = @as(usize, @intCast(offset)) + 8;
    if (start + length > entry.len) return error.EndOfStream;
    return entry[start..][0..length];
}
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
    const version = frameVersion(record.changes);
    try out.append(a, version);
    try appendInt(&out, a, u64, record.scan_epoch orelse std.math.maxInt(u64));
    try appendInt(&out, a, u32, @intCast(record.changes.len));
    for (record.changes) |change| {
        if (change.key.len == 0 or change.key.len > 512 or internalKey(change.key)) return error.InvalidTransactionSessionRecord;
        const size = change.key.len +| (if (change.expected) |value| value.len else 0) +| (if (change.value) |value| value.len else 0);
        if (size +| out.items.len +| (if (version >= 3) @as(usize, 13) else 12) > max_bytes) return error.TransactionTooLarge;
        if (version >= 3) try out.append(a, @as(u8, @intFromBool(change.expected_is_digest)) | (@as(u8, @intFromBool(change.value_is_splice)) << 1));
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
    const version = try reader.int(u8);
    if (version != 1 and version != 2 and version != 3 and version != 4) return error.InvalidTransactionSessionRecord;
    const epoch = try reader.int(u64);
    const count = try reader.int(u32);
    if (count > max_operations) return error.TransactionTooLarge;
    const changes = try a.alloc(Change, count);
    errdefer a.free(changes);
    for (changes, 0..) |*change, i| {
        const compare = if (version >= 3) try reader.int(u8) else 0;
        if (compare > (if (version == 4) @as(u8, 3) else 1)) return error.InvalidTransactionSessionRecord;
        const key = (try reader.slice()) orelse return error.InvalidTransactionSessionRecord;
        if (key.len == 0 or key.len > 512 or internalKey(key)) return error.InvalidTransactionSessionRecord;
        for (changes[0..i]) |previous| if (std.mem.eql(u8, key, previous.key)) return error.InvalidTransactionSessionRecord;
        change.* = .{ .key = key, .expected = try reader.slice(), .value = try reader.slice(), .expected_is_digest = compare & 1 != 0, .value_is_splice = compare & 2 != 0 };
        if (change.expected_is_digest and (change.expected == null or change.expected.?.len != Digest.digest_length)) return error.InvalidTransactionSessionRecord;
        if (change.value_is_splice and (!change.expected_is_digest or change.value == null or change.value.?.len < 12 or !std.mem.startsWith(u8, key, session_prefix)))
            return error.InvalidTransactionSessionRecord;
    }
    if (version != frameVersion(changes) or reader.pos != bytes.len) return error.InvalidTransactionSessionRecord;
    return .{ .changes = changes, .scan_epoch = if (epoch == std.math.maxInt(u64)) null else epoch };
}
/// Reuse the largest unchanged portion of a sealed receipt's staged data
/// or execution plan. Validate the entire base value with the ordinary CAS.
pub fn receiptSplice(a: std.mem.Allocator, previous: []const u8, replacement: []const u8) !?[]const u8 {
    var old_offset: usize = 0;
    var new_offset: usize = 0;
    var shared: usize = 0;
    for ([_][]const u8{ ",\"staged\":", ",\"execution_plan\":" }) |anchor| {
        const old_start = std.mem.indexOf(u8, previous, anchor) orelse continue;
        const new_start = std.mem.indexOf(u8, replacement, anchor) orelse continue;
        const old = previous[old_start..];
        const new = replacement[new_start..];
        var same: usize = 0;
        while (same < @min(old.len, new.len) and old[same] == new[same]) : (same += 1) {}
        if (same > shared) {
            old_offset = old_start;
            new_offset = new_start;
            shared = same;
        }
    }
    if (shared <= 12 or replacement.len > max_bytes + max_receipt_growth) return null;
    var header: [12]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], @intCast(old_offset), .little);
    std.mem.writeInt(u32, header[4..8], @intCast(new_offset), .little);
    std.mem.writeInt(u32, header[8..12], @intCast(shared), .little);
    return try std.mem.concat(a, u8, &.{ &header, replacement[0..new_offset], replacement[new_offset + shared ..] });
}

fn expandReceiptSplice(a: std.mem.Allocator, previous: []const u8, patch: []const u8) ![]u8 {
    const old_offset: usize = std.mem.readInt(u32, patch[0..4], .little);
    const new_offset: usize = std.mem.readInt(u32, patch[4..8], .little);
    const shared: usize = std.mem.readInt(u32, patch[8..12], .little);
    const edits = patch[12..];
    if (old_offset > previous.len or shared > previous.len - old_offset or new_offset > edits.len or
        edits.len +| shared > max_bytes + max_receipt_growth) return error.InvalidTransactionSessionRecord;
    return std.mem.concat(a, u8, &.{ edits[0..new_offset], previous[old_offset..][0..shared], edits[new_offset..] });
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
    var supplied_count = false;
    var writes_count = false;
    var membership_changed = false;
    for (record.changes) |change| {
        const key = try keyAlloc(a, group, change.key);
        defer a.free(key);
        const is_count = std.mem.eql(u8, change.key, count_key);
        if (is_count) {
            supplied_count = true;
            writes_count = change.value != null;
        }
        if (std.mem.startsWith(u8, change.key, session_prefix) and (change.expected == null) != (change.value == null)) membership_changed = true;
        // CAS always uses the canonical bytes, including on replicas whose
        // derived watermark was last maintained by a different binary.
        const actual = try optionalGet(txn, key);
        if (change.expected) |expected| {
            const current = actual orelse return false;
            if (change.expected_is_digest) {
                var digest: [Digest.digest_length]u8 = undefined;
                Digest.hash(current, &digest, .{});
                if (!std.mem.eql(u8, &digest, expected)) return false;
            } else if (!std.mem.eql(u8, current, expected)) return false;
        } else if (actual != null) return false;
    }
    if (record.changes.len == 0) return true;
    const next = std.math.add(u64, epoch, 1) catch return error.InvalidTransactionSessionRecord;
    const marker_key = try keyAlloc(a, group, watermark_key);
    defer a.free(marker_key);
    const gen = (try generation(txn, marker_key, epoch)) orelse next;
    const count_stamp = try keyAlloc(a, group, count_watermark);
    defer a.free(count_stamp);
    const carry_count = !supplied_count and !membership_changed and try countIsCurrent(txn, a, group, epoch);
    for (record.changes) |change| {
        const key = try keyAlloc(a, group, change.key);
        defer a.free(key);
        const expanded = if (change.value_is_splice)
            try expandReceiptSplice(a, (try optionalGet(txn, key)) orelse return error.InvalidTransactionSessionRecord, change.value.?)
        else
            null;
        defer if (expanded) |bytes_owned| a.free(bytes_owned);
        const replacement = if (expanded) |value| value else change.value;
        if (replacement) |value| try txn.put(key, value) else txn.delete(key) catch |err| {
            if (err != error.NotFound) return err;
        };
        if (std.mem.startsWith(u8, change.key, session_prefix)) {
            const entry_key = try cacheKey(a, group, change.key);
            defer a.free(entry_key);
            if (replacement) |value| {
                var entry: [8 + cached_prefix_bytes]u8 = undefined;
                std.mem.writeInt(u64, entry[0..8], gen, .little);
                const n = @min(value.len, cached_prefix_bytes);
                @memcpy(entry[8..][0..n], value[0..n]);
                try txn.put(entry_key, entry[0 .. 8 + n]);
            } else txn.delete(entry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }
    var marker: [16]u8 = undefined;
    std.mem.writeInt(u64, marker[0..8], next, .little);
    std.mem.writeInt(u64, marker[8..16], gen, .little);
    try txn.put(epoch_physical, marker[0..8]);
    try txn.put(marker_key, &marker);
    if (writes_count or carry_count) try txn.put(count_stamp, marker[0..8]);
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
    var digest: [Digest.digest_length]u8 = undefined;
    Digest.hash("pending", &digest, .{});
    const compact = try encode(a, .{ .changes = &.{.{ .key = "receipt", .expected = &digest, .expected_is_digest = true, .value = "committed" }} });
    defer a.free(compact);
    try std.testing.expectEqual(@as(u8, 3), compact[0]);
    try std.testing.expect(try apply(&map, a, 7, compact));
    try std.testing.expect(!try apply(&map, a, 7, compact));
    const old_plan = "{\"id\":1,\"execution_plan\":\"retained plan that is longer than the splice header\"}";
    const new_plan = "{\"id\":1,\"execution_plan\":\"retained plan that is longer than the splice header\",\"terminal\":true}";
    Digest.hash(old_plan, &digest, .{});
    const patch = (try receiptSplice(a, old_plan, new_plan)).?;
    defer a.free(patch);
    const session_key = session_prefix ++ "splice";
    const physical_session_key = try keyAlloc(a, 7, session_key);
    defer a.free(physical_session_key);
    try map.put(physical_session_key, old_plan);
    const spliced = try encode(a, .{ .changes = &.{.{ .key = session_key, .expected = &digest, .value = patch, .expected_is_digest = true, .value_is_splice = true }} });
    defer a.free(spliced);
    try std.testing.expectEqual(@as(u8, 4), spliced[0]);
    try std.testing.expect(try apply(&map, a, 7, spliced));
    try std.testing.expectEqualStrings(new_plan, try map.get(physical_session_key));
    try std.testing.expect(!try apply(&map, a, 7, spliced));
    // Restore the fixture for the remaining epoch tests.
    try map.put(key, "pending");
    const other = try keyAlloc(a, 7, "other");
    defer a.free(other);
    try std.testing.expectError(error.NotFound, map.get(other));
    try std.testing.expectEqualStrings("pending", try map.get(key));
    const phantom = try encode(a, .{ .scan_epoch = 0, .changes = &.{.{ .key = "receipt", .expected = "pending", .value = "committed" }} });
    defer a.free(phantom);
    try std.testing.expect(!try apply(&map, a, 7, phantom));
    const committed = try encode(a, .{ .scan_epoch = 3, .changes = &.{.{ .key = "receipt", .expected = "pending", .value = "committed" }} });
    defer a.free(committed);
    try std.testing.expect(try apply(&map, a, 7, committed));
    try std.testing.expectEqualStrings("committed", try map.get(key));
    try std.testing.expect(try apply(&map, a, 8, first));

    const session = session_prefix ++ "abc";
    const create = try encode(a, .{ .changes = &.{.{ .key = session, .expected = null, .value = "original header and payload" }} });
    defer a.free(create);
    try std.testing.expect(try apply(&map, a, 9, create));
    // A cache hit may only use small projection keys, even on a backend with
    // no range-read method. An unrelated write preserves the session's entry.
    const PrefixOnly = struct {
        map: *Map,
        full: []const u8,
        pub fn get(self: *@This(), k: []const u8) ![]const u8 {
            if (std.mem.eql(u8, k, self.full)) return error.UnexpectedFullRead;
            return self.map.get(k);
        }
    };
    const full = try keyAlloc(a, 9, session);
    defer a.free(full);
    var scoped: PrefixOnly = .{ .map = &map, .full = full };
    try std.testing.expect(try apply(&map, a, 9, first));
    try std.testing.expectEqualStrings("header", (try cachedRange(&scoped, a, 9, session, 9, 6)).?);
    const epoch = try keyAlloc(a, 9, epoch_key);
    defer a.free(epoch);
    // Simulate an old apply binary rewriting the canonical record and epoch.
    try map.put(full, "legacy replacement");
    try map.put(epoch, &std.mem.toBytes(std.mem.nativeToLittle(u64, 3)));
    try std.testing.expect((try cachedRange(&scoped, a, 9, session, 0, 7)) == null);
    const other_session = try encode(a, .{ .changes = &.{.{ .key = session_prefix ++ "def", .expected = null, .value = "other" }} });
    defer a.free(other_session);
    try std.testing.expect(try apply(&map, a, 9, other_session));
    try std.testing.expect((try cachedRange(&scoped, a, 9, session, 0, 7)) == null);
    const replace = try encode(a, .{ .changes = &.{.{ .key = session, .expected = "legacy replacement", .value = "current" }} });
    defer a.free(replace);
    try std.testing.expect(try apply(&map, a, 9, replace));
    try std.testing.expectEqualStrings("current", (try cachedRange(&scoped, a, 9, session, 0, 7)).?);
    try std.testing.expectError(error.EndOfStream, cachedRange(&scoped, a, 9, session, 0, 8));
    const remove = try encode(a, .{ .changes = &.{.{ .key = session, .expected = "current", .value = null }} });
    defer a.free(remove);
    try std.testing.expect(try apply(&map, a, 9, remove));
    try std.testing.expect((try cachedRange(&scoped, a, 9, session, 0, 7)) == null);
    // A pre-counter writer can change membership, and a pre-counter apply
    // binary can advance the epoch without touching the count watermark.
    const one = std.mem.toBytes(std.mem.nativeToLittle(u64, 1));
    const bootstrap = try encode(a, .{ .changes = &.{.{ .key = count_key, .expected = null, .value = &one }} });
    defer a.free(bootstrap);
    try std.testing.expect(try apply(&map, a, 9, bootstrap));
    const physical_count = try keyAlloc(a, 9, count_key);
    defer a.free(physical_count);
    const before = std.mem.readInt(u64, (try map.get(epoch))[0..8], .little);
    try std.testing.expect(try countIsCurrent(&map, a, 9, before));
    try std.testing.expect(try apply(&map, a, 9, create));
    try std.testing.expect(!try countIsCurrent(&map, a, 9, before + 1));
    try std.testing.expectEqualSlices(u8, &one, try map.get(physical_count));
    // Repair uses the same raw CAS on followers with and without a watermark.
    const two = std.mem.toBytes(std.mem.nativeToLittle(u64, 2));
    const repair = try encode(a, .{ .scan_epoch = before + 1, .changes = &.{.{ .key = count_key, .expected = &one, .value = &two }} });
    defer a.free(repair);
    try std.testing.expect(try apply(&map, a, 9, repair));
    try std.testing.expect(try countIsCurrent(&map, a, 9, before + 2));
    const legacy_epoch = std.mem.toBytes(std.mem.nativeToLittle(u64, 100));
    try map.put(epoch, &legacy_epoch);
    try std.testing.expect(!try countIsCurrent(&map, a, 9, 100));
}
