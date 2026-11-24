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

//! Physical row placement inside the same primary KV transaction. Logical row
//! keys remain stable; private heap keys sort by rewrite generation and ordinal.
//! Resolving a location always uses the caller's pinned transaction, so normal
//! MVCC/manifest ownership also protects its payload. No source lease is opened
//! after observing a location, and physical relocation changes no row revision.
const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const keys = @import("internal_keys.zig");
const A = std.mem.Allocator;
pub const magic = "AFRHEAP1";
const family = "AFRHEAP";
pub const reference_len = 52;
pub const key_prefix = "\x04\x01";
pub const key_len = key_prefix.len + 16 + 8;
pub const Generation = [16]u8;
/// Physical view with caller-owned lifetime. Cursor adapters retain the actual
/// backend transaction pointer, never an address of a copied facade value.
pub const Reader = struct {
    ptr: *anyopaque,
    read: *const fn (*anyopaque, []const u8) anyerror![]const u8,
    pub fn get(self: Reader, key: []const u8) ![]const u8 {
        return self.read(self.ptr, key);
    }
    pub fn from(comptime T: type, ptr: *T) Reader {
        const Bridge = struct {
            fn get(raw: *anyopaque, key: []const u8) anyerror![]const u8 {
                return (@as(*T, @ptrCast(@alignCast(raw)))).get(key);
            }
        };
        return .{ .ptr = ptr, .read = Bridge.get };
    }
};

pub const metadata_prefix = "\x00\x00__physical_row_heap__:";
pub const enabled_key = metadata_prefix ++ "enabled";
pub const retirement_prefix = metadata_prefix ++ "retired:";
pub const retirement_clock_key = metadata_prefix ++ "clock";
pub const retirement_bytes_key = metadata_prefix ++ "retired_bytes";
pub const retirement_key_len = retirement_prefix.len + 8;
pub const maximum_retired_bytes: u64 = 64 * 1024 * 1024;

pub fn isPhysicalMetadata(key: []const u8) bool {
    return std.mem.startsWith(u8, key, metadata_prefix);
}
fn counter(reader: anytype, key: []const u8) !u64 {
    const value = reader.get(key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    if (value.len != 8) return error.CorruptRowHeapCatalog;
    return std.mem.readInt(u64, value[0..8], .little);
}
fn putCounter(writer: anytype, key: []const u8, value: u64) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    try writer.put(key, &bytes);
}
pub fn retirementClock(reader: anytype) !u64 {
    return counter(reader, retirement_clock_key);
}
pub fn retiredBytes(reader: anytype) !u64 {
    return counter(reader, retirement_bytes_key);
}
fn retirementCharge(ref: Reference) u64 {
    return @as(u64, ref.value_len) + key_len + reference_len + retirement_key_len;
}
fn retire(writer: anytype, ref: Reference) !void {
    const next = std.math.add(u64, try retirementClock(writer), 1) catch return error.RowHeapClockExhausted;
    var key: [retirement_key_len]u8 = undefined;
    @memcpy(key[0..retirement_prefix.len], retirement_prefix);
    std.mem.writeInt(u64, key[retirement_prefix.len..][0..8], next, .big);
    const record = ref.encode();
    try writer.put(&key, &record);
    try putCounter(writer, retirement_clock_key, next);
    const bytes = std.math.add(u64, try retiredBytes(writer), retirementCharge(ref)) catch return error.RowHeapClockExhausted;
    try putCounter(writer, retirement_bytes_key, bytes);
}

/// Collection may delete only a fence established before advancing probe
/// admission, after every preceding current-tip probe has released its epoch.
/// Stable snapshots use the backend's ordinary MVCC/manifest ownership instead.
pub fn collectRetired(writer: anytype, retirement_key: []const u8, fence: u64) !u64 {
    if (retirement_key.len != retirement_key_len or !std.mem.startsWith(u8, retirement_key, retirement_prefix)) return error.CorruptRowHeapCatalog;
    if (std.mem.readInt(u64, retirement_key[retirement_prefix.len..][0..8], .big) > fence) return error.RowHeapCollectionFence;
    const raw = try writer.get(retirement_key);
    const ref = try decodeLocation(raw);
    const physical = ref.physicalKey();
    try writer.delete(&physical);
    try writer.delete(retirement_key);
    const remaining = std.math.sub(u64, try retiredBytes(writer), retirementCharge(ref)) catch return error.CorruptRowHeapCatalog;
    try putCounter(writer, retirement_bytes_key, remaining);
    return retirementCharge(ref);
}

pub fn isPhysicalKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, key_prefix);
}
pub fn isReference(value: []const u8) bool {
    return std.mem.startsWith(u8, value, family);
}
fn ownerHash(key: []const u8) [8]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(key, &digest, .{});
    return digest[0..8].*;
}
pub const Reference = struct {
    generation: Generation,
    ordinal: u64,
    value_len: u32,
    value_checksum: u32,
    owner: [8]u8,

    pub fn physicalKey(self: Reference) [key_len]u8 {
        var result: [key_len]u8 = undefined;
        @memcpy(result[0..key_prefix.len], key_prefix);
        @memcpy(result[key_prefix.len..][0..16], &self.generation);
        std.mem.writeInt(u64, result[key_prefix.len + 16 ..][0..8], self.ordinal, .big);
        return result;
    }
    pub fn encode(self: Reference) [reference_len]u8 {
        var result: [reference_len]u8 = undefined;
        @memcpy(result[0..8], magic);
        @memcpy(result[8..24], &self.generation);
        std.mem.writeInt(u64, result[24..32], self.ordinal, .big);
        std.mem.writeInt(u32, result[32..36], self.value_len, .little);
        std.mem.writeInt(u32, result[36..40], self.value_checksum, .little);
        @memcpy(result[40..48], &self.owner);
        std.mem.writeInt(u32, result[48..52], Crc32.hash(result[0..48]), .little);
        return result;
    }
    pub fn verifyValue(self: Reference, value: []const u8) !void {
        if (value.len != self.value_len or Crc32.hash(value) != self.value_checksum or isReference(value)) return error.CorruptRowHeapValue;
    }
};

pub fn decodeLocation(value: []const u8) !Reference {
    if (value.len != reference_len or !std.mem.eql(u8, value[0..8], magic)) return error.CorruptRowHeapReference;
    if (Crc32.hash(value[0..48]) != std.mem.readInt(u32, value[48..52], .little) or std.mem.allEqual(u8, value[8..24], 0)) return error.CorruptRowHeapReference;
    return .{
        .generation = value[8..24].*,
        .ordinal = std.mem.readInt(u64, value[24..32], .big),
        .value_len = std.mem.readInt(u32, value[32..36], .little),
        .value_checksum = std.mem.readInt(u32, value[36..40], .little),
        .owner = value[40..48].*,
    };
}
pub fn decode(key: []const u8, value: []const u8) !?Reference {
    if (!isReference(value) or !keys.isStoredDocumentRowKey(key)) return null;
    const ref = try decodeLocation(value);
    if (!std.mem.eql(u8, &ref.owner, &ownerHash(key))) return error.CorruptRowHeapReference;
    return ref;
}

/// The reader is a physical view of the SAME transaction that yielded raw.
/// Never fall back to a stale inline value when the referenced payload is bad.
pub fn resolve(reader: anytype, key: []const u8, raw: []const u8) ![]const u8 {
    const ref = (try decode(key, raw)) orelse return raw;
    const physical = ref.physicalKey();
    const value = reader.get(&physical) catch |err| switch (err) {
        error.NotFound => return error.DanglingRowHeapReference,
        else => return err,
    };
    try ref.verifyValue(value);
    return value;
}

pub fn location(key: []const u8, value: []const u8, generation: Generation, ordinal: u64) !Reference {
    if (!keys.isStoredDocumentRowKey(key) or std.mem.allEqual(u8, &generation, 0) or isReference(value)) return error.InvalidRowHeapPlacement;
    return .{ .generation = generation, .ordinal = ordinal, .value_len = if (value.len > std.math.maxInt(u32)) return error.RowHeapValueTooLarge else @intCast(value.len), .value_checksum = Crc32.hash(value), .owner = ownerHash(key) };
}

/// Move one logical row without invoking logical-mutation/version hooks. The
/// payload, location, old-payload retirement and the job cursor commit together.
/// Callers must fence logical writers and publish their cursor in this writer.
pub fn relocate(a: A, writer: anytype, key: []const u8, generation: Generation, ordinal: u64) !u64 {
    const previous = try writer.get(key);
    const old = try decode(key, previous);
    const value = try resolve(writer, key, previous);
    const owned = try a.dupe(u8, value);
    defer a.free(owned);
    if (old) |ref| if (std.mem.eql(u8, &ref.generation, &generation) and ref.ordinal == ordinal) return owned.len;
    if (try retiredBytes(writer) > maximum_retired_bytes) return error.RowHeapCollectionBusy;
    const next = try location(key, owned, generation, ordinal);
    const physical = next.physicalKey();
    // A reused destination must belong to this exact ordinal/row. Silently
    // overwriting another row's payload would corrupt its retained location.
    if (writer.get(&physical)) |_| {
        if (old == null or !std.mem.eql(u8, &old.?.physicalKey(), &physical)) return error.RowHeapPlacementConflict;
    } else |err| if (err != error.NotFound) return err;
    try writer.put(enabled_key, magic);
    try writer.put(&physical, owned);
    const encoded = next.encode();
    try writer.put(key, &encoded);
    if (old) |ref| {
        const prior = ref.physicalKey();
        if (!std.mem.eql(u8, &prior, &physical)) try retire(writer, ref);
    }
    return owned.len;
}

/// CLUSTER is a one-time placement. Ordinary updates become inline deltas;
/// they do not allocate another immutable heap body on every churn cycle.
/// Retired bodies remain visible until old current-tip probes have drained.
pub fn put(writer: anytype, key: []const u8, value: []const u8) !void {
    if (keys.isStoredDocumentRowKey(key)) {
        if (isReference(value)) return error.InvalidRowHeapPlacement;
        const raw = writer.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (raw) |before| if (try decode(key, before)) |old| try retire(writer, old);
    }
    return writer.put(key, value);
}
pub fn remove(writer: anytype, key: []const u8) !void {
    if (keys.isStoredDocumentRowKey(key)) {
        const raw = writer.get(key) catch |err| switch (err) {
            error.NotFound => return writer.delete(key),
            else => return err,
        };
        if (try decode(key, raw)) |ref| try retire(writer, ref);
    }
    try writer.delete(key);
}

/// Keep the key lookup batch logical, then read payloads in physical order.
/// Reassigning by original ordinal preserves caller ordering and duplicates.
pub fn resolveMany(a: A, reader: anytype, logical: []const []const u8, values: []?[]const u8) !void {
    if (logical.len != values.len) return error.InvalidBatch;
    var count: usize = 0;
    for (logical, values) |key, value| if (value) |raw| {
        if ((try decode(key, raw)) != null) count += 1;
    };
    if (count == 0) return;
    const Item = struct {
        original: usize,
        ref: Reference,
        key: [key_len]u8,
        fn less(_: void, lhs: @This(), rhs: @This()) bool {
            return std.mem.order(u8, &lhs.key, &rhs.key) == .lt;
        }
    };
    const items = try a.alloc(Item, count);
    defer a.free(items);
    var at: usize = 0;
    for (logical, values, 0..) |key, value, i| if (value) |raw| {
        if (try decode(key, raw)) |ref| {
            items[at] = .{ .original = i, .ref = ref, .key = ref.physicalKey() };
            at += 1;
        }
    };
    std.mem.sort(Item, items, {}, Item.less);
    const physical_keys = try a.alloc([]const u8, count);
    defer a.free(physical_keys);
    const physical_values = try a.alloc(?[]const u8, count);
    defer a.free(physical_values);
    for (items, physical_keys) |*item, *key| key.* = &item.key;
    try reader.getManySorted(physical_keys, physical_values);
    for (items, physical_values) |item, raw| {
        const value = raw orelse return error.DanglingRowHeapReference;
        try item.ref.verifyValue(value);
        values[item.original] = value;
    }
}

/// Protected by the store's existing reader-admission mutex. Only current-tip
/// probes need epochs: stable read transactions already retain both KV planes.
/// At most one retired epoch is pending, so long probes cannot create a growing
/// array of epoch objects. Rewrite admission bounds debt while one is delayed.
pub const Epochs = struct {
    current: u64 = 1,
    readers: [2]usize = .{ 0, 0 },
    retiring: ?u64 = null,
    fence: u64 = 0,
    pub fn acquire(self: *Epochs) !u64 {
        const slot: usize = @intCast(self.current & 1);
        self.readers[slot] = std.math.add(usize, self.readers[slot], 1) catch return error.RowHeapReaderLimit;
        return self.current;
    }
    pub fn release(self: *Epochs, epoch: u64) void {
        std.debug.assert(epoch == self.current or self.retiring == epoch);
        const slot: usize = @intCast(epoch & 1);
        std.debug.assert(self.readers[slot] != 0);
        self.readers[slot] -= 1;
    }
    pub fn mark(self: *Epochs, committed_fence: u64) !void {
        if (self.retiring != null) return;
        const next = std.math.add(u64, self.current, 1) catch return error.RowHeapClockExhausted;
        std.debug.assert(self.readers[@intCast(next & 1)] == 0);
        self.retiring = self.current;
        self.current = next;
        self.fence = committed_fence;
    }
    pub fn collectible(self: *const Epochs) ?u64 {
        const old = self.retiring orelse return null;
        return if (self.readers[@intCast(old & 1)] == 0) self.fence else null;
    }
    pub fn finish(self: *Epochs, fence: u64) !void {
        if (self.collectible() != fence) return error.RowHeapCollectionFence;
        self.retiring = null;
    }
};

test "row heap epochs keep late probes moving while retired payloads wait for old probes" {
    var epochs: Epochs = .{};
    const old = try epochs.acquire();
    try epochs.mark(7);
    const next = try epochs.acquire();
    try std.testing.expect(old != next);
    try std.testing.expect(epochs.collectible() == null);
    epochs.release(old);
    try std.testing.expectEqual(@as(?u64, 7), epochs.collectible());
    try epochs.finish(7);
    // The new reader did not block collecting bodies it could never observe.
    try epochs.mark(9);
    try std.testing.expect(epochs.collectible() == null);
    epochs.release(next);
    try std.testing.expectEqual(@as(?u64, 9), epochs.collectible());
    try epochs.finish(9);
}

test "row heap references bind owner bytes payload checksum generation and physical order" {
    const a = std.testing.allocator;
    const key = try keys.documentKeyAlloc(a, "row:a");
    defer a.free(key);
    const other = try keys.documentKeyAlloc(a, "row:b");
    defer a.free(other);
    const ref = try location(key, "payload", @splat(1), 3);
    var bytes = ref.encode();
    try std.testing.expectEqualDeep(ref, (try decode(key, &bytes)).?);
    try ref.verifyValue("payload");
    try std.testing.expectError(error.CorruptRowHeapValue, ref.verifyValue("changed"));
    try std.testing.expectError(error.CorruptRowHeapReference, decode(other, &bytes));
    bytes[25] ^= 1;
    try std.testing.expectError(error.CorruptRowHeapReference, decode(key, &bytes));
    try std.testing.expect((try decode("artifact", &ref.encode())) == null);
    const later = try location(key, "payload", @splat(1), 256);
    try std.testing.expect(std.mem.order(u8, &ref.physicalKey(), &later.physicalKey()) == .lt);
    try std.testing.expectError(error.InvalidRowHeapPlacement, location(key, "payload", @splat(0), 1));
}
