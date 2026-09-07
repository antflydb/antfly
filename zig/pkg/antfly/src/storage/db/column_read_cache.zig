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

//! Snapshot-local, bounded reuse of verified immutable column payloads. No
//! backend slices, schema pointers, or materialized JSON escape a block here.
const std = @import("std");
const dv = @import("../../section/typed_doc_values.zig");
const payloads = @import("column_payloads.zig");
const Allocator = std.mem.Allocator;

pub const max_rows = 256;

pub fn cellBytes(value: dv.TypedValue) u64 {
    return 4 + switch (value) {
        .bytes_val => |bytes| @as(u64, bytes.len) + 4,
        .bool_val => @as(u64, 1),
        .geo_point => @as(u64, 16),
        .numeric_val => @as(u64, 9),
        else => @as(u64, 8),
    };
}

/// Single-threaded ownership: one reference per active block plus at most one
/// cache reference. Byte values borrow only the owned decompressed chunks.
pub const Payload = struct {
    alloc: Allocator,
    references: usize = 1,
    value_type: dv.ValueType,
    values: []?dv.TypedValue,
    chunks: [][]u8,
    encoded_bytes: u64,
    logical_bytes: u64,
    retained_bytes: usize,

    pub fn decode(alloc: Allocator, encoded: []const u8, ref: payloads.Ref) !*Payload {
        try ref.validate(encoded);
        if (ref.source_rows == 0 or ref.source_rows > max_rows) return error.InvalidColumnSegment;
        var reader = try dv.TypedDocValuesReader.init(alloc, encoded[0 .. encoded.len - 4]);
        var chunks = std.ArrayListUnmanaged([]u8).empty;
        defer chunks.deinit(alloc);
        errdefer for (chunks.items) |bytes| alloc.free(bytes);
        var values: [max_rows]?dv.TypedValue = @splat(null);
        var extent: usize = 0;
        var logical_bytes: u64 = 0;
        var chunk_bytes: usize = 0;
        for (0..reader.num_chunks) |index| {
            var chunk = try reader.decodeChunk(@intCast(index));
            var owned = false;
            defer if (!owned) chunk.deinit();
            var it = chunk.iterator();
            while (try it.next()) |entry| {
                if (entry.doc_id >= ref.source_rows or values[entry.doc_id] != null) return error.InvalidColumnSegment;
                values[entry.doc_id] = entry.value;
                logical_bytes += cellBytes(entry.value);
                extent = @max(extent, entry.doc_id + 1);
            }
            if (reader.value_type == .bytes_val) {
                try chunks.append(alloc, chunk.data);
                chunk_bytes += chunk.data.len;
                owned = true;
            }
        }
        if (!std.mem.eql(u8, &payloads.identity(reader.value_type, values[0..extent]), &ref.digest)) return error.InvalidColumnSegment;
        const result = try alloc.create(Payload);
        errdefer alloc.destroy(result);
        const owned_values = try alloc.dupe(?dv.TypedValue, values[0..extent]);
        errdefer alloc.free(owned_values);
        const owned_chunks = try chunks.toOwnedSlice(alloc);
        result.* = .{
            .alloc = alloc,
            .value_type = reader.value_type,
            .values = owned_values,
            .chunks = owned_chunks,
            .encoded_bytes = encoded.len,
            .logical_bytes = logical_bytes,
            .retained_bytes = @sizeOf(Payload) + owned_values.len * @sizeOf(?dv.TypedValue) + owned_chunks.len * @sizeOf([]u8) + chunk_bytes,
        };
        return result;
    }

    pub fn retain(self: *Payload) void {
        self.references += 1;
    }

    pub fn release(self: *Payload) void {
        self.references -= 1;
        if (self.references != 0) return;
        for (self.chunks) |bytes| self.alloc.free(bytes);
        self.alloc.free(self.chunks);
        self.alloc.free(self.values);
        self.alloc.destroy(self);
    }
};

pub const Stats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    admissions: u64 = 0,
    evictions: u64 = 0,
    bypasses: u64 = 0,
    peak_bytes: usize = 0,
};

/// Four-way set associativity bounds lookup work and metadata independently of
/// scan length. LRU replacement skips pinned entries. The budget includes the
/// entire lazy slot table and requested allocations for retained payloads, not
/// allocator bookkeeping or the current block's unavoidable decode workspace.
/// One instance belongs to exactly one read snapshot and manifest generation.
/// This intentionally has no global lock, cross-query pollution, or invalidation.
pub const Cache = struct {
    alloc: Allocator,
    budget: usize,
    slots: ?*[slot_count]Slot = null,
    retained_bytes: usize = 0,
    clock: u64 = 0,
    stats: Stats = .{},

    const ways = 4;
    const sets = 16;
    const slot_count = ways * sets;
    const Slot = struct { digest: [32]u8 = undefined, payload: ?*Payload = null, touched: u64 = 0 };

    pub fn init(alloc: Allocator, budget: usize) Cache {
        return .{ .alloc = alloc, .budget = budget };
    }

    pub fn deinit(self: *Cache) void {
        if (self.slots) |slots| {
            for (slots) |slot| if (slot.payload) |payload| payload.release();
            self.alloc.destroy(slots);
        }
        self.* = undefined;
    }

    fn setStart(digest: [32]u8) usize {
        return @as(usize, digest[0] % sets) * ways;
    }

    /// Returns a pinned reference owned by the caller.
    pub fn get(self: *Cache, digest: [32]u8) ?*Payload {
        if (self.slots) |slots| {
            const first = setStart(digest);
            for (slots[first..][0..ways]) |*slot| if (slot.payload) |payload| {
                if (std.mem.eql(u8, &slot.digest, &digest)) {
                    self.clock +|= 1;
                    slot.touched = self.clock;
                    payload.retain();
                    self.stats.hits += 1;
                    return payload;
                }
            };
        }
        self.stats.misses += 1;
        return null;
    }

    pub fn contains(self: *const Cache, digest: [32]u8) bool {
        const slots = self.slots orelse return false;
        const first = setStart(digest);
        for (slots[first..][0..ways]) |slot| {
            if (slot.payload != null and std.mem.eql(u8, &slot.digest, &digest)) return true;
        }
        return false;
    }

    fn evict(self: *Cache, slot: *Slot) void {
        const payload = slot.payload.?;
        std.debug.assert(payload.references == 1);
        self.retained_bytes -= payload.retained_bytes;
        payload.release();
        slot.payload = null;
        self.stats.evictions += 1;
    }

    /// Best effort: OOM, oversized payloads, and pinned pressure bypass the
    /// cache without failing a valid scan. The caller retains its own reference.
    pub fn admit(self: *Cache, digest: [32]u8, payload: *Payload) void {
        const table_bytes = @sizeOf([slot_count]Slot);
        if (self.contains(digest)) return;
        if (table_bytes > self.budget or payload.retained_bytes > self.budget - table_bytes) {
            self.stats.bypasses += 1;
            return;
        }
        if (self.slots == null) {
            const slots = self.alloc.create([slot_count]Slot) catch {
                self.stats.bypasses += 1;
                return;
            };
            slots.* = @splat(.{});
            self.slots = slots;
            self.retained_bytes = table_bytes;
        }
        const slots = self.slots.?;
        const first = setStart(digest);
        var target: ?*Slot = null;
        for (slots[first..][0..ways]) |*slot| {
            if (slot.payload == null) {
                target = slot;
                break;
            }
            if (slot.payload.?.references == 1 and (target == null or slot.touched < target.?.touched)) target = slot;
        }
        const destination = target orelse {
            self.stats.bypasses += 1;
            return;
        };
        // Check reclaimable capacity first: an unsuccessful admission must not
        // flush useful entries when active block pins prevent meeting the budget.
        var available = self.budget - self.retained_bytes;
        if (destination.payload) |old| available += old.retained_bytes;
        // Ordinary same-sized set replacements need only four-way lookup,
        // not a global cache walk. Inspect other sets only under byte pressure.
        if (available < payload.retained_bytes) for (slots) |*slot| {
            if (slot == destination) continue;
            if (slot.payload) |old| if (old.references == 1) {
                available += old.retained_bytes;
                if (available >= payload.retained_bytes) break;
            };
        };
        if (available < payload.retained_bytes) {
            self.stats.bypasses += 1;
            return;
        }
        if (destination.payload != null) self.evict(destination);
        while (payload.retained_bytes > self.budget - self.retained_bytes) {
            var oldest: ?*Slot = null;
            for (slots) |*slot| if (slot.payload) |old| {
                if (old.references == 1 and (oldest == null or slot.touched < oldest.?.touched)) oldest = slot;
            };
            self.evict(oldest.?);
        }
        self.clock +|= 1;
        payload.retain();
        destination.* = .{ .digest = digest, .payload = payload, .touched = self.clock };
        self.retained_bytes += payload.retained_bytes;
        self.stats.peak_bytes = @max(self.stats.peak_bytes, self.retained_bytes);
        self.stats.admissions += 1;
    }
};

fn testEncoded(alloc: Allocator, values: []const ?dv.TypedValue, value_type: dv.ValueType) !struct { bytes: []u8, ref: payloads.Ref } {
    // Multiple chunks exercise ownership across decompression buffers.
    var writer = dv.TypedDocValuesWriter.init(alloc, value_type, 1);
    defer writer.deinit();
    for (values, 0..) |value, i| if (value) |cell| try writer.add(@intCast(i), cell);
    const raw = try writer.build();
    defer alloc.free(raw);
    const bytes = try alloc.alloc(u8, raw.len + 4);
    @memcpy(bytes[0..raw.len], raw);
    std.mem.writeInt(u32, bytes[raw.len..][0..4], std.hash.Crc32.hash(raw), .little);
    return .{ .bytes = bytes, .ref = .{ .digest = payloads.identity(value_type, values), .bytes = bytes.len, .source_rows = @intCast(values.len) } };
}

fn testDecodeAllocations(alloc: Allocator, encoded: []const u8, ref: payloads.Ref) !void {
    const payload = try Payload.decode(alloc, encoded, ref);
    defer payload.release();
    try std.testing.expectEqualStrings("first", payload.values[0].?.bytes_val);
    try std.testing.expect(payload.values[1] == null);
    try std.testing.expectEqualStrings("last", payload.values[2].?.bytes_val);
}

test "column read cache payload owns all chunks and cleans up allocation failures" {
    const alloc = std.testing.allocator;
    const encoded = try testEncoded(alloc, &.{ .{ .bytes_val = "first" }, null, .{ .bytes_val = "last" } }, .bytes_val);
    defer alloc.free(encoded.bytes);
    try std.testing.checkAllAllocationFailures(alloc, testDecodeAllocations, .{ encoded.bytes, encoded.ref });
    const payload = try Payload.decode(alloc, encoded.bytes, encoded.ref);
    defer payload.release();
    @memset(encoded.bytes, 0);
    try std.testing.expectEqualStrings("first", payload.values[0].?.bytes_val);
    try std.testing.expectEqualStrings("last", payload.values[2].?.bytes_val);
    try std.testing.expectError(error.InvalidColumnSegment, Payload.decode(alloc, encoded.bytes, encoded.ref));
}

test "column read cache verifies digest and row extent before admission" {
    const alloc = std.testing.allocator;
    const encoded = try testEncoded(alloc, &.{ .{ .i64_val = 3 }, .{ .i64_val = 4 } }, .i64_val);
    defer alloc.free(encoded.bytes);
    var ref = encoded.ref;
    ref.digest[0] ^= 1;
    try std.testing.expectError(error.InvalidColumnSegment, Payload.decode(alloc, encoded.bytes, ref));
    ref = encoded.ref;
    ref.source_rows = 1;
    try std.testing.expectError(error.InvalidColumnSegment, Payload.decode(alloc, encoded.bytes, ref));
    const payload = try Payload.decode(alloc, encoded.bytes, encoded.ref);
    defer payload.release();
    try std.testing.expectEqual(@as(usize, 0), payload.chunks.len); // Scalars do not retain decompression buffers.
}

test "column read cache bounds memory skips pinned victims and releases owned bytes" {
    const alloc = std.testing.allocator;
    const encoded = try testEncoded(alloc, &.{.{ .bytes_val = "owned" }}, .bytes_val);
    defer alloc.free(encoded.bytes);
    const first = try Payload.decode(alloc, encoded.bytes, encoded.ref);
    var first_owned = true;
    defer if (first_owned) first.release();
    const second = try Payload.decode(alloc, encoded.bytes, encoded.ref);
    defer second.release();
    const budget = @sizeOf([Cache.slot_count]Cache.Slot) + first.retained_bytes;
    var cache = Cache.init(alloc, budget);
    defer cache.deinit();
    cache.admit(encoded.ref.digest, first);
    try std.testing.expectEqual(budget, cache.retained_bytes);
    var second_key = encoded.ref.digest;
    second_key[1] ^= 1; // Same set, distinct cache key, to exercise replacement.
    cache.admit(second_key, second);
    try std.testing.expectEqual(@as(u64, 1), cache.stats.bypasses);
    try std.testing.expectEqual(@as(u64, 0), cache.stats.evictions);
    first.release();
    first_owned = false;
    cache.admit(second_key, second);
    try std.testing.expectEqual(@as(u64, 1), cache.stats.evictions);
    try std.testing.expect(cache.get(encoded.ref.digest) == null);
    const pinned = cache.get(second_key).?;
    defer pinned.release();
    try std.testing.expectEqualStrings("owned", pinned.values[0].?.bytes_val);
    try std.testing.expect(cache.stats.peak_bytes <= budget);
}

test "column read cache bypasses disabled oversized and failed allocations" {
    const alloc = std.testing.allocator;
    const encoded = try testEncoded(alloc, &.{.{ .bytes_val = "owned" }}, .bytes_val);
    defer alloc.free(encoded.bytes);
    const payload = try Payload.decode(alloc, encoded.bytes, encoded.ref);
    defer payload.release();
    for ([_]usize{ 0, payload.retained_bytes }) |budget| {
        var cache = Cache.init(alloc, budget);
        defer cache.deinit();
        cache.admit(encoded.ref.digest, payload);
        try std.testing.expect(cache.slots == null);
        try std.testing.expectEqual(@as(u64, 1), cache.stats.bypasses);
        try std.testing.expectEqual(@as(usize, 1), payload.references);
    }
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var cache = Cache.init(failing.allocator(), 1024 * 1024);
    defer cache.deinit();
    cache.admit(encoded.ref.digest, payload);
    try std.testing.expectEqual(@as(u64, 1), cache.stats.bypasses);
    try std.testing.expectEqual(@as(usize, 1), payload.references);
}

test "column read cache set replacement is LRU and accounting includes all owned storage" {
    const alloc = std.testing.allocator;
    const encoded = try testEncoded(alloc, &.{.{ .bytes_val = "owned" }}, .bytes_val);
    defer alloc.free(encoded.bytes);
    var measured = std.testing.FailingAllocator.init(alloc, .{});
    var cache = Cache.init(measured.allocator(), 1024 * 1024);
    defer cache.deinit();
    var digests: [5][32]u8 = @splat(encoded.ref.digest);
    for (&digests, 0..) |*digest, i| digest[1] = @intCast(i);
    for (digests[0..4]) |digest| {
        const payload = try Payload.decode(measured.allocator(), encoded.bytes, encoded.ref);
        defer payload.release();
        cache.admit(digest, payload);
    }
    try std.testing.expectEqual(cache.retained_bytes, measured.allocated_bytes - measured.freed_bytes);
    const pin = cache.get(digests[0]).?;
    pin.release();
    const replacement = try Payload.decode(measured.allocator(), encoded.bytes, encoded.ref);
    cache.admit(digests[4], replacement);
    replacement.release();
    try std.testing.expect(cache.contains(digests[0]));
    try std.testing.expect(!cache.contains(digests[1]));
    try std.testing.expect(cache.contains(digests[4]));
    try std.testing.expectEqual(@as(u64, 1), cache.stats.evictions);
    try std.testing.expectEqual(cache.retained_bytes, measured.allocated_bytes - measured.freed_bytes);
}

test "column read cache pins survive cache destruction" {
    const alloc = std.testing.allocator;
    const encoded = try testEncoded(alloc, &.{.{ .bytes_val = "owned" }}, .bytes_val);
    defer alloc.free(encoded.bytes);
    var pinned: *Payload = undefined;
    {
        var cache = Cache.init(alloc, 1024 * 1024);
        defer cache.deinit();
        const payload = try Payload.decode(alloc, encoded.bytes, encoded.ref);
        defer payload.release();
        cache.admit(encoded.ref.digest, payload);
        pinned = cache.get(encoded.ref.digest).?;
    }
    defer pinned.release();
    try std.testing.expectEqual(@as(usize, 1), pinned.references);
    try std.testing.expectEqualStrings("owned", pinned.values[0].?.bytes_val);
}
